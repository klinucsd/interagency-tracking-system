import os
import numpy as np
import pandas as pd
import geopandas as gpd
import psutil
import sys
import math
import logging
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
    
    
        

    # scale up by centroid of polygon geometry if reported acre is larger than geometry area
    final_gdf['ACRE_RATIO'] = final_gdf.ACTIVITY_QUANTITY* 4046.86/(final_gdf.geometry.area)
    final_gdf.geometry = final_gdf.apply(buffer_overreport_poly, axis = 1)
    final_gdf = final_gdf.drop('ACRE_RATIO', axis=1)
    
    
    logger.info(f"      final polygons: {final_gdf.shape}")
    
    return final_gdf



def get_footprint(input_append_path, 
                  enriched_point_layer_name,
                  enriched_line_layer_name,
                  enriched_polygon_layer_name,
                  output_report_path,
                  veg_path,
                  veg_layer_name,
                  start_year,
                  end_year):
    

    # read in input files
    veg_gdf = gpd.read_file(veg_path, driver='OpenFileGDB', layer=veg_layer_name)

    enriched_points = gpd.read_file(input_append_path, driver='OpenFileGDB', layer=enriched_point_layer_name)
    enriched_lines = gpd.read_file(input_append_path, driver='OpenFileGDB', layer=enriched_line_layer_name)
    enriched_polygons = gpd.read_file(input_append_path, driver='OpenFileGDB', layer=enriched_polygon_layer_name)

    # buffer data by activity quantity acre to the exact amount of acres
    buffered_pt = update_pt(enriched_points)
    buffered_ln = update_ln(enriched_lines)
    buffered_poly = update_poly(enriched_polygons)

    # process footprint by year
    # iterate through data from input start year to input end year
    for y in list(map(str, range(start_year, end_year+1))):
        logger.info(f"      Processing footprint report for year {y}")
        poly_cur_y = buffered_poly[buffered_poly['Year_txt'] == y]
        ln_cur_y = buffered_ln[buffered_ln['Year_txt'] == y]
        pt_cur_y = buffered_pt[buffered_pt['Year_txt'] == y]
        
        # timber nonspatial should not follow the spatial operations below
        # take reported value at face value
        # append later to final result
        timber_cur_y = pt_cur_y[pt_cur_y.AGENCY == 'TIMBER']

        
        # concat enriched data together
        footprint_cur_y = pd.concat([poly_cur_y, ln_cur_y, pt_cur_y[pt_cur_y.AGENCY != 'TIMBER']], ignore_index=True)
        
        # drop unecessary fields, dissolve by treatment and take max activity quantity 
        # 05/08/2025 UPDATE: subject to change
        dissolved_cur_y = footprint_cur_y[['TRMTID_USER', 'ACTIVITY_QUANTITY', 'geometry']].dissolve('TRMTID_USER', aggfunc='max')
        
        # append the first instance of agency value to associated treatment id
        # used later to manually assign onwership for caltrans data specifically
        agency = footprint_cur_y.set_index(footprint_cur_y.TRMTID_USER).loc[dissolved_cur_y.index, 'AGENCY']
        
        dissolved_cur_y['AGENCY'] = agency[~agency.index.duplicated()]
        
        # make valid would split a invalid polygon to multiple valid polygons and convert to MultiPolygon type
        dissolved_cur_y.geometry = dissolved_cur_y.make_valid()
        
        # simplified SIG Meatball&Speghetti function
        # use spatial join to rule out high overlapping
        ##############################################################################################################
        
        # create "meatballs" by taking the representative point of footprint polygons
        dissolved_point = dissolved_cur_y.copy()
        dissolved_point.geometry = dissolved_point.representative_point()
        
        # perform spatial join to find intersecting point/polygon
        joined_data = gpd.sjoin(dissolved_point, dissolved_cur_y, how='left', predicate='intersects').reset_index()
        
        # for polygon with multiple overlapping points, find the Treatment ID of those points whose Activity Quantity
        # is smaller than the polygon's Activity Quantity
        footprint_mask = joined_data[joined_data['ACTIVITY_QUANTITY_left'] < joined_data['ACTIVITY_QUANTITY_right']]['TRMTID_USER_left']
        
        # remove these non-qualifying treatments from footprint, essentially the same as taking max for overlapping
        footpring_gdf = dissolved_cur_y.reset_index()
        footpring_gdf = footpring_gdf[~footpring_gdf['TRMTID_USER'].isin(footprint_mask)]
        
        ##############################################################################################################

        # dask spatial join to enrich vegetation, ownership, etc.
        # use representative point for faster sjoin
        footprint_pts = footpring_gdf.copy()
        footprint_pts.geometry = footprint_pts.representative_point()
        ddf = dask_geopandas.from_geopandas(footprint_pts, npartitions=16)
        # TODO: some points may fall outside of veg, ownership layer?
        footprint_enriched = ddf.sjoin(veg_gdf, how='inner', predicate='intersects').compute()
        
        # manually assign CALTRANS ownership to state
        footprint_enriched.loc[footprint_enriched.AGENCY=='CALSTA', 'PRIMARY_OWNERSHIP_GROUP'] = 'STATE'
        
        # reassign the polygon geometry back to enriched footprint
        footprint_enriched.geometry = footpring_gdf.geometry
        # drop artifact columns
        footprint_enriched = footprint_enriched.drop(['index_right', 'Shape_Length', 'Shape_Area'], axis=1)
        
        # assign timber 
        timber_cur_y['WUI'] = 'Non-WUI'
        
        # append timber back to footprint
        footprint_enriched = pd.concat([footprint_enriched, timber_cur_y[footprint_enriched.columns]])
        
        # save to file
        save_gdf_to_gdb(footprint_enriched, output_report_path, 'footprint' + y)




if __name__ == "__main__":
        # Get the current process ID
    process = psutil.Process(os.getpid())


    # read file path from configuration yaml file
    import yaml
    with open("..\config.yaml", 'r') as stream:
        config_inputs = yaml.safe_load(stream)

    input_append_path = config_inputs['appended']['gdb_path']
    enriched_point_layer_name = config_inputs['appended']['point_layer_name']
    enriched_line_layer_name = config_inputs['appended']['line_layer_name']
    enriched_polygon_layer_name = config_inputs['appended']['polygon_layer_name']
    output_report_path = config_inputs['footprint']['gdb_path']
    veg_path = config_inputs['global']['veg_own_gdb']
    veg_layer_name = config_inputs['global']['veg_own_layer_name']
    start_year = config_inputs['footprint']['start_year']
    end_year = config_inputs['footprint']['end_year']

    get_footprint(input_append_path, 
                  enriched_point_layer_name,
                  enriched_line_layer_name,
                  enriched_polygon_layer_name,
                  output_report_path,
                  veg_path,
                  veg_layer_name,
                  start_year,
                  end_year)

    # Get memory usage in bytes, convert to MB
    memory_usage = process.memory_info().rss / 1024 / 1024
    logger.info(f"Memory usage: {memory_usage:.2f} MB")