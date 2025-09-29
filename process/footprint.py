import os
import numpy as np
import pandas as pd
import geopandas as gpd
import psutil
import sys
import math
import logging
import yaml

# currently require arcpy for nop 
# WIP
# need to be resolved to limit to geopandas environment in the future
import arcpy
sys.path.append('../')

import dask_geopandas
# some data need to be converted to multi-type again
from utils.save_gdf_to_gdb import save_gdf_to_gdb
#from process.footprint_report import update_ln, update_poly, update_pt
from shapely import Polygon, MultiPolygon
from shapely import affinity

from its_logging.logger_config import logger
logger = logging.getLogger('process.footprint')


def buffer_overreport_poly(row):
    """
     buffer polygons that have reported activity quantity larger than geometrical area
     buffer the polygon geometry to the reported acres by scaling to ratio
    """

    # TODO: Keep GIS area take max of both

    if row.ACRE_RATIO <= 1:
        return row.geometry
    
    # scale up by centroid if type is polygon
    if isinstance(row.geometry, Polygon):
        return affinity.scale(row.geometry, xfact=np.sqrt(row.ACRE_RATIO), yfact=np.sqrt(row.ACRE_RATIO))
    # scale up by centroid of each polygon in multi-polygon
    else:
        out = []
        for geom in list(row.geometry.geoms):
            out.append(affinity.scale(geom, xfact=np.sqrt(row.ACRE_RATIO), yfact=np.sqrt(row.ACRE_RATIO)))
        return MultiPolygon(out)


def update_pt(enriched_points: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    """Update points with buffer calculations."""
    logger.info(f"      initial points count: {len(enriched_points)}")
    
    # Add BufferMeters field
    enriched_points['BufferMeters'] = None
    
    # Calculate buffer for AC units
    mask1 = (enriched_points['ACTIVITY_QUANTITY'].notna()) & (enriched_points['ACTIVITY_UOM'] == 'AC')
    logger.info(f"      points with valid ACTIVITY_QUANTITY and AC units: {mask1.sum()}")
    
    # Buffer point by their reported activity quantity 
    enriched_points.loc[mask1, 'BufferMeters'] = np.sqrt((enriched_points.loc[mask1, 'ACTIVITY_QUANTITY'] * 4046.86) / math.pi)
    
    # Filter for COUNTS_TO_MAS
    mask2 = (enriched_points['COUNTS_TO_MAS'] == 'YES') & (enriched_points['BufferMeters'].notna())
    logger.info(f"      points with COUNTS_TO_MAS='YES' and valid BufferMeters: {mask2.sum()}")
    
    selected_points = enriched_points[mask2].copy()

    selected_points = selected_points[selected_points['BufferMeters'] > 0]
    logger.info(f"      points with BufferMeters > 0: {len(selected_points)}")
    
    # Create buffers
    buffered_geoms = selected_points.apply(
        lambda row: row.geometry.buffer(row['BufferMeters']) if pd.notnull(row['BufferMeters']) else row.geometry,
        axis=1
    )
    
    # Create new GeoDataFrame with buffered geometries
    result = selected_points.copy()
    result.geometry = buffered_geoms
    logger.info(f"      final points count: {len(result)}")
    return result


def update_ln(enriched_lines: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    """Update lines with buffer calculations."""
    logger.info(f"      initial lines count: {len(enriched_lines)}")
        
    # Check for case differences
    if 'ACTIVITY_UOM' in enriched_lines.columns:
        unique_uom = enriched_lines['ACTIVITY_UOM'].unique()
        logger.info(f"      unique ACTIVITY_UOM values: {unique_uom}")
    
    # Add BufferMeters field
    enriched_lines['BufferMeters'] = None
    
    # Calculate line lengths
    line_lengths = enriched_lines.geometry.length
    logger.info(f"      lines with length > 0: {(line_lengths > 0).sum()}")
    
    # Modify conditions to be more lenient and check for case variations
    condition1 = enriched_lines['ACTIVITY_QUANTITY'].notna() if 'ACTIVITY_QUANTITY' in enriched_lines.columns else False
    logger.info(f"      lines with valid ACTIVITY_QUANTITY: {condition1.sum() if isinstance(condition1, pd.Series) else 0}")
    
    condition2 = enriched_lines['ACTIVITY_UOM'].str.upper() == 'AC' if 'ACTIVITY_UOM' in enriched_lines.columns else False
    logger.info(f"      lines with ACTIVITY_UOM = 'AC' (case insensitive): {condition2.sum() if isinstance(condition2, pd.Series) else 0}")
    
    # Check combined conditions
    mask1 = condition1 & condition2 & (line_lengths > 0)
    logger.info(f"      lines meeting all conditions: {mask1.sum() if isinstance(mask1, pd.Series) else 0}")
    
    if not isinstance(mask1, pd.Series) or mask1.sum() == 0:
        logger.warning("      no lines meet the filtering criteria. Please check if the required columns exist and contain expected values.")
        return enriched_lines[enriched_lines['geometry'].notna()].head(0)  # Return empty GeoDataFrame with same schema
    
    # Calculate buffer distances
    enriched_lines.loc[mask1, 'BufferMeters'] = (
        (enriched_lines.loc[mask1, 'ACTIVITY_QUANTITY'] * 4046.86) / 
        line_lengths[mask1] / 2
    )
    
    # Check COUNTS_TO_MAS condition
    condition3 =  (enriched_lines['COUNTS_TO_MAS'] == 'YES') & (enriched_lines['BufferMeters'].notna())
    logger.info(f"      lines with COUNTS_TO_MAS = 'YES': {condition3.sum() if isinstance(condition3, pd.Series) else 0}")
    
    condition4 = ~((enriched_lines['BufferMeters'] >= 200) & (enriched_lines['Source'] == 'CalTrans'))
    
    # Final filter
    mask2 = condition3 & condition4
    selected_lines = enriched_lines[mask2].copy()
    
    # caltrans activity is merged with treatment_id/geometry pair
    # safe to only take max to reduce dissolve process time
    caltrans = get_max_treatment(selected_lines[selected_lines.Source == 'CALTRANS'])
    selected_lines = pd.concat([selected_lines[selected_lines.Source != 'CALTRANS'], caltrans],ignore_index=True)
    
    
    # Create buffers
    buffered_geoms = selected_lines.apply(
        lambda row: row.geometry.buffer(row['BufferMeters'], cap_style='flat'),
        axis=1
    )
    
    # Create new GeoDataFrame with buffered geometries
    result = selected_lines.copy()
    result.geometry = buffered_geoms
    logger.info(f"      final lines count: {len(result)}")
    
    return result


def update_poly(enriched_polygons: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    """Update polygons with selection criteria."""
    logger.info(f"      initial polygons count: {len(enriched_polygons)}")

    mas_gdf = enriched_polygons[enriched_polygons['COUNTS_TO_MAS'] == 'YES']
    logger.info(f"      polygons with COUNTS_TO_MAS = 'YES': {mas_gdf.shape}")
    
    # inherited from arcpy version
    # from logger there seems to be no such anomoly
    treatment_gdf = enriched_polygons[enriched_polygons['TREATMENT_AREA'] < 100000]
    logger.info(f"      polygons with 'TREATMENT_AREA' < 100000: {treatment_gdf.shape}")
    
    mask = (
        (enriched_polygons['COUNTS_TO_MAS'] == 'YES') & 
        (enriched_polygons['TREATMENT_AREA'] < 100000)
    )

    final_gdf = enriched_polygons[mask].copy()
    
    
        
    
    
    logger.info(f"      final polygons: {final_gdf.shape}")
    
    return final_gdf


def enrich_dissolved_columns(enrich_cols, df, dissolved_df):
    df_max = df.sort_values('ACTIVITY_QUANTITY').drop_duplicates('TRMTID_USER', keep='last')
    df_max = df_max.set_index(df_max['TRMTID_USER']).loc[dissolved_df.index, enrich_cols]
    dissolved_df.loc[:, enrich_cols] = df_max
    return dissolved_df

# author: James Falter @ARB
# Function to remove duplicate geometries with different coordinate vectors
def remove_duplicates(gdf):
    normalized_wkb = gdf.geometry.apply(lambda geom: geom.normalize().wkb)
    unique_gdf = gdf.loc[~normalized_wkb.duplicated()].reset_index(drop=True)

    return unique_gdf


def get_footprint(input_append_path, 
                  enriched_point_layer_name,
                  enriched_line_layer_name,
                  enriched_polygon_layer_name,
                  output_report_path,
                  temp_path,
                  veg_path,
                  veg_layer_name,
                  start_year,
                  end_year):
    

    # read in input files
    veg_gdf = gpd.read_file(veg_path, driver='OpenFileGDB', layer=veg_layer_name)

    enriched_points = gpd.read_file(input_append_path, driver='OpenFileGDB', layer=enriched_point_layer_name)
    enriched_lines = gpd.read_file(input_append_path, driver='OpenFileGDB', layer=enriched_line_layer_name)
    enriched_polygons = gpd.read_file(input_append_path, driver='OpenFileGDB', layer=enriched_polygon_layer_name)

    
    enrich_cols = ['AGENCY', 'IN_WUI', 'PRIMARY_OWNERSHIP_GROUP', 'BROAD_VEGETATION_TYPE', 'REGION', 'COUNTY']

    # buffer data by activity quantity acre to the exact amount of acres
    buffered_pt = update_pt(enriched_points)
    buffered_ln = update_ln(enriched_lines)
    buffered_poly = update_poly(enriched_polygons)

    # process footprint by year
    # iterate through data from input start year to input end year
    for y in list(map(str, range(start_year, end_year+1))):
        poly_cur_y = buffered_poly[buffered_poly['Year_txt'] == y]
        # put ln 
        ln_cur_y = buffered_ln[buffered_ln['Year_txt'] == y]
        pt_cur_y = buffered_pt[buffered_pt['Year_txt'] == y]
        
        # timber nonspatial should not follow the spatial operations below
        # take reported value at face value
        # append later to final result
        timber_cur_y = pt_cur_y[pt_cur_y.AGENCY == 'TIMBER']
        
        # concat polygon and buffered points data
        poly_pt = pd.concat([poly_cur_y, pt_cur_y[pt_cur_y.AGENCY != 'TIMBER']], ignore_index=True)
        # find the union geometry
        poly_pt_union_geom = poly_pt.dissolve().geometry[0]
        # interesect line data with union(polygon, point) to find lines that have overlap with other data
        intersect_mask = ln_cur_y.intersects(poly_pt_union_geom)


        # find non-intersecting lines
        ln_noi = ln_cur_y[~intersect_mask]
        # dissolve by treatmentid and find max acre as footprint
        dissolved_ln_noi = ln_noi[['TRMTID_USER', 'ACTIVITY_QUANTITY', 'geometry']].dissolve('TRMTID_USER', aggfunc='max')
        # re-attach original cols
        enrich_dissolved_columns(enrich_cols, ln_noi, dissolved_ln_noi)
        # this will be directly used along with TIMBER in final footprint

        # find interesecting lines
        ln_intersect = ln_cur_y[intersect_mask]
        # dissolve by treatmentid and find max acre as footprint
        dissolved_ln_int = ln_intersect[['TRMTID_USER', 'ACTIVITY_QUANTITY', 'geometry']].dissolve('TRMTID_USER', aggfunc='max')
        # re-attach original cols
        enrich_dissolved_columns(enrich_cols, ln_intersect, dissolved_ln_int)
        
        # find non intersecting area ratio and apply to treatment to approximate footprint
        footprint_ln_int = dissolved_ln_int.ACTIVITY_QUANTITY*(dissolved_ln_int.difference(poly_pt_union_geom).area/dissolved_ln_int.area)
        dissolved_ln_int['ACTIVITY_QUANTITY'] = footprint_ln_int
        # this will be directly used along with TIMBER in final footprint
        
        
        # prep for NOP application for polygon and buffered points
        # drop unecessary fields, dissolve by treatment and take max activity quantity 
        dissolved_cur_y = poly_pt[['TRMTID_USER', 'ACTIVITY_QUANTITY', 'geometry']].dissolve('TRMTID_USER', aggfunc='max')
        
        
        # append the max instance of enrich col value to associated treatment id
        enrich_dissolved_columns(enrich_cols, poly_pt, dissolved_cur_y)

        dissolved_cur_y = dissolved_cur_y.reset_index()
        
        # make valid would split a invalid polygon to multiple valid polygons and convert to MultiPolygon type
        dissolved_cur_y.geometry = dissolved_cur_y.make_valid()
        
        dissolved_cur_y = remove_duplicates(dissolved_cur_y)
        
        
        # create "meatballs" by taking the representative point of footprint polygons
        dissolved_point = dissolved_cur_y.copy()
        dissolved_point.geometry = dissolved_point.representative_point()
        
        save_gdf_to_gdb(dissolved_cur_y, temp_path, 'original'+y)
        save_gdf_to_gdb(dissolved_cur_y[['geometry']].explode(), temp_path, 'spaghetti'+y)
        save_gdf_to_gdb(dissolved_point, temp_path, 'meatball'+y)
        save_gdf_to_gdb(pd.concat([timber_cur_y, dissolved_ln_int.reset_index(), dissolved_ln_noi.reset_index()], ignore_index=True), temp_path, 'timber'+y)

    # arcpy process the nop
    get_nop_arcpy(temp_path, start_year, end_year)
    # back to geopandas to process final output
    get_footprint_p2(output_report_path, temp_path, start_year, end_year)

    # delete temp file
    






def get_nop_arcpy(temp_path, start_year, end_year):
    for y in list(map(str, range(start_year, end_year+1))):
        # create clean output file
        Spaghetti_FeatureToPolygon = os.path.join(temp_path, "Spaghetti_FeatureToPolygon"+y)

        # use arcpy feature to polygon tool to create non overlapping polygons
        arcpy.management.FeatureToPolygon(
            in_features=[os.path.join(temp_path, "spaghetti"+y)], 
            out_feature_class=Spaghetti_FeatureToPolygon
        )
        # check results
        result = arcpy.management.GetCount(Spaghetti_FeatureToPolygon)
        print("{} has {} records".format(Spaghetti_FeatureToPolygon, result[0]))


def get_footprint_p2(report_path, temp_path, start_year, end_year):
    for y in list(map(str, range(start_year, end_year+1))):
        # read in temp files
        spaghetti = gpd.read_file(temp_path, driver='OpenFileGDB', layer='Spaghetti_FeatureToPolygon' + y)
        dissolved_point = gpd.read_file(temp_path, driver='OpenFileGDB', layer='meatball' + y)
        dissolved_cur_y = gpd.read_file(temp_path, driver='OpenFileGDB', layer='original' + y)
        timber_cur_y = gpd.read_file(temp_path, driver='OpenFileGDB', layer='timber' + y)
        
        print('all file read')
        
        # keep all NOP by left join
        footprint_temp = spaghetti.sjoin(dissolved_point, how='left', predicate='intersects').reset_index()
        # keep max value of each NOP
        footprint_temp = footprint_temp.dropna().sort_values('ACTIVITY_QUANTITY').drop_duplicates('TRMTID_USER', keep='last')
        # find original geometry with treatment id
        footprint_gdf = dissolved_cur_y.set_index('TRMTID_USER').loc[footprint_temp.TRMTID_USER].reset_index()
        # concat with timber
        footprint_enriched = pd.concat([footprint_gdf, timber_cur_y[footprint_gdf.columns]])
        # save to file
        save_gdf_to_gdb(footprint_enriched, report_path, 'Footprint_Report_'+ y)





if __name__ == "__main__":
        # Get the current process ID
    process = psutil.Process(os.getpid())


    # read file path from configuration yaml file
    with open("..\config.yaml", 'r') as stream:
        config_inputs = yaml.safe_load(stream)

    input_append_path = config_inputs['appended']['gdb_path']
    enriched_point_layer_name = config_inputs['appended']['point_layer_name']
    enriched_line_layer_name = config_inputs['appended']['line_layer_name']
    enriched_polygon_layer_name = config_inputs['appended']['polygon_layer_name']
    output_report_path = config_inputs['footprint']['gdb_path']
    veg_path = config_inputs['global']['veg_own_gdb']
    veg_layer_name = config_inputs['global']['veg_own_layer_name']
    temp_path = config_inputs['footprint']['temp_path']
    start_year = config_inputs['footprint']['start_year']
    end_year = config_inputs['footprint']['end_year']

    get_footprint(input_append_path, 
                  enriched_point_layer_name,
                  enriched_line_layer_name,
                  enriched_polygon_layer_name,
                  output_report_path,
                  temp_path,
                  veg_path,
                  veg_layer_name,
                  start_year,
                  end_year)

    # Get memory usage in bytes, convert to MB
    memory_usage = process.memory_info().rss / 1024 / 1024
    logger.info(f"Memory usage: {memory_usage:.2f} MB")