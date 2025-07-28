import os
import numpy as np
import pandas as pd
import geopandas as gpd
import psutil
import sys
import math
import logging
from tqdm import tqdm
from multiprocessing import Pool

sys.path.append('../')


# some data need to be converted to multi-type again
from save_gdf_to_gdb import save_gdf_to_gdb
#from process.footprint_report import update_ln, update_poly, update_pt
from shapely import Polygon, MultiPolygon
from shapely import affinity

from logger_config import logger
logger = logging.getLogger('process.footprint')

global GDF_CUR
global INTERSECTION_LIST
GDF_CUR = None
INTERSECTION_LIST = None

def get_max_treatment(gdf):
    '''
    only use on specific agencies where treatment is reported with the same geometry
    '''
    max_idx = gdf.groupby(['TRMTID_USER', 'Year_txt'])['ACTIVITY_QUANTITY'].idxmax()
    return gdf.loc[max_idx]

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
    
    
        

    # scale up by centroid of polygon geometry if reported acre is larger than geometry area
    final_gdf['ACRE_RATIO'] = final_gdf.ACTIVITY_QUANTITY* 4046.86/(final_gdf.geometry.area)
    final_gdf.geometry = final_gdf.apply(buffer_overreport_poly, axis = 1)
    final_gdf = final_gdf.drop('ACRE_RATIO', axis=1)
    
    
    logger.info(f"      final polygons: {final_gdf.shape}")
    
    return final_gdf


def flatten(container):
    for i in container:
        if isinstance(i, (list,tuple)):
            for j in flatten(i):
                yield j
        else:
            yield i

# author: James Falter @ARB
# Function to find local intersections between a target feature and a set of features
def get_interaction_list(gdf):
    # Build spatial index for efficient bounding box queries
    sindex = gdf.sindex

    # Store results in a pre-formatted list
    interaction_gdf_list = [None]*len(gdf)

    for idx, row in gdf.iterrows():
        # Get the bounding box of the current feature
        bounds = row.geometry.bounds

        # Find possible neighbors using bounding box intersection
        possible_neighbors = list(sindex.intersection(bounds))

        # Remove self from possible neighbors
        possible_neighbors = [i for i in possible_neighbors if i != idx]

        # Check for actual intersection with each possible neighbor
        intersecting_neighbors = [
            i for i in possible_neighbors
            if row.geometry.intersects(gdf.loc[i].geometry)
        ]

        # Assign results if not empty
        if len(intersecting_neighbors) > 0:
            interaction_gdf_list[idx] = gdf.iloc[intersecting_neighbors]
        else:
            interaction_gdf_list[idx] = []

    return interaction_gdf_list


# author: James Falter @ARB
# Function to remove duplicate geometries with different coordinate vectors
def remove_duplicates(gdf):
    normalized_wkb = gdf.geometry.apply(lambda geom: geom.normalize().wkb)
    unique_gdf = gdf.loc[~normalized_wkb.duplicated()].reset_index(drop=True)

    return unique_gdf


def get_NOP_recur(gdf1, gdf2):
    gdf1 = gdf1[gdf1.geom_type.isin(['MultiPolygon', 'Polygon'])]
    gdf2 = gdf2[gdf2.geom_type.isin(['MultiPolygon', 'Polygon'])]

    # keep_geom_type=False will often time produce artifact in geometry of line/point
    # only keep polygon/multipolygon
    intersection = gpd.overlay(gdf1, gdf2, how='intersection', keep_geom_type=False)
    intersection = intersection[intersection.geom_type.isin(['MultiPolygon', 'Polygon'])]

    difference = gpd.overlay(gdf1, gdf2, how='difference', keep_geom_type=False)
    difference = difference[difference.geom_type.isin(['MultiPolygon', 'Polygon'])]

    temp_nop = pd.concat([intersection, difference])
    if len(temp_nop) <= 1:
        return [gdf1]
    else:
        return [get_NOP_recur(temp_nop.iloc[[i]], 
                              pd.concat([temp_nop.iloc[0:i],temp_nop.iloc[i+1:]])) for i in range(len(temp_nop))]
    


def get_NOP_mp(i):
    logger.info(f"      processing item {i}")
    if len(INTERSECTION_LIST[i]) == 0:
        logger.info(f"      completed item {i}")
        return(GDF_CUR.iloc[[i]])
    nop_cur = get_NOP_recur(GDF_CUR.iloc[[i]], INTERSECTION_LIST[i])
    nop_cur = pd.concat(flatten(nop_cur)).drop_duplicates('geometry')
    nop_cur = nop_cur[~np.isclose(nop_cur.area, 0)]

    logger.info(f"      completed item {i}")

    return nop_cur



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
    #veg_gdf = gpd.read_file(veg_path, driver='OpenFileGDB', layer=veg_layer_name)

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
        
        ##############################################################################################################
        logger.info(f"      Multiprocessing NOP")

        global GDF_CUR
        GDF_CUR = dissolved_cur_y.reset_index()

        global INTERSECTION_LIST
        INTERSECTION_LIST = get_interaction_list(GDF_CUR)

        with Pool(processes=8) as pool:
            # Process groups in parallel
            results = pool.map(get_NOP_mp, range(len(GDF_CUR)))
        

        
        # save to file
        save_gdf_to_gdb(pd.concat(results, ignore_index=True), output_report_path, 'footprint' + y)




if __name__ == "__main__":
        # Get the current process ID
    process = psutil.Process(os.getpid())


    # read file path from configuration yaml file
    import yaml
    with open("config.yaml", 'r') as stream:
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