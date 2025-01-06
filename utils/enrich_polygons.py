
import os
import time
import logging
import pandas as pd
import geopandas as gpd
import numpy as np

from multiprocessing import Pool
from datetime import datetime
from concurrent.futures import ProcessPoolExecutor, as_completed
from multiprocessing import Pool

from its_logging.logger_config import logger
from utils.gdf_utils import show_columns, hash_geodataframe
from utils.keep_fields import keep_fields
from utils.year import calculate_fiscal_years
from utils.crosswalk import crosswalk

from utils.concurrent_join import split_gdf

logger = logging.getLogger('utils.enrich_polygons')


# Global variables
in_polygons = None
in_sum_features_filtered = None

def init_globals(polygons_df, features_df):
    """Initialize global variables for multiprocessing"""
    global in_polygons, in_sum_features_filtered
    in_polygons = polygons_df
    in_sum_features_filtered = features_df

def process_group(idx):
    """Process a single group of joined features"""
    global in_polygons, in_sum_features_filtered
    
    group = joined[joined['Join_ID'] == idx]
    intersection_areas = []
    total_area = 0
    polygon_count = 0
    
    for index, row in group.iterrows():
        treatment_geom = row['geometry']
        matching_features = in_sum_features_filtered.loc[in_sum_features_filtered['Id'] == row['Id'], 'geometry']
        
        if matching_features.empty:
            continue
            
        veg_type_geom = in_sum_features_filtered.loc[in_sum_features_filtered['Id'] == row['Id'], 'geometry'].iloc[0]
        intersection_geom = treatment_geom.intersection(veg_type_geom)
        area_acres = intersection_geom.area * 0.000247105
        
        total_area += area_acres
        polygon_count += 1
        
        intersection_areas.append({
            'WHR13NAME': row['WHR13NAME'],
            'area_acres': area_acres
        })
    
    if intersection_areas:
        veg_areas_df = pd.DataFrame(intersection_areas)
        veg_summary = veg_areas_df.groupby('WHR13NAME')['area_acres'].sum().reset_index()
        
        if not veg_summary.empty:
            dominant_veg = veg_summary.loc[veg_summary['area_acres'].idxmax(), 'WHR13NAME']
            return idx, {
                'dominant_veg': dominant_veg,
                'sum_Area_ACRES': total_area,
                'Polygon_Count': int(polygon_count)  # Explicitly cast to integer
            }
    
    return idx, {
        'dominant_veg': None,
        'sum_Area_ACRES': total_area,
        'Polygon_Count': int(polygon_count)  # Explicitly cast to integer
    }

def process_spatial_join_parallel(in_polygons_df, in_sum_features_filtered_df, n_processes=None):
    """Main function to process spatial join in parallel"""
    global joined
    
    # Perform the spatial join
    logger.info(f"            enrich step 4/32 joining with board veg types")
    joined = gpd.sjoin(in_sum_features_filtered_df, in_polygons_df, how='right', predicate='intersects')
    logger.info(f"               joined records: {joined.shape[0]}")
    show_columns(logger, joined, "joined")

    # Get unique Join_IDs
    unique_ids = joined['Join_ID'].unique()

    # Initialize pool with globals
    logger.info(f"            enrich step 5/32 concurrent calculate veg type for each polygon")
    with Pool(processes=n_processes, 
             initializer=init_globals, 
             initargs=(in_polygons_df, in_sum_features_filtered_df)) as pool:
        
        # Process groups in parallel
        results = pool.map(process_group, unique_ids)
    
    # Convert results to dictionary
    results_dict = dict(results)
    
    # Update in_polygons with results
    logger.info(f"            enrich step 6/32 assign veg type for each polygon")
    for idx, data in results_dict.items():
        mask = in_polygons_df['Join_ID'] == idx
        in_polygons_df.loc[mask, 'BROAD_VEGETATION_TYPE'] = data['dominant_veg']
        in_polygons_df.loc[mask, 'sum_Area_ACRES'] = data['sum_Area_ACRES']
        in_polygons_df.loc[mask, 'Polygon_Count'] = data['Polygon_Count']
    
    # Convert Polygon_Count column to integer type
    in_polygons_df['Polygon_Count'] = in_polygons_df['Polygon_Count'].astype(int)

    return in_polygons_df


def summarize_within(in_polygons, in_sum_features, group_field='WHR13NAME'):
    """
    Replicates ArcPy's SummarizeWithin functionality using GeoPandas
    Specifically handles data in CRS 3310 (California Albers)
    
    Parameters:
    in_polygons (GeoDataFrame): The polygon features to summarize within (in CRS 3310)
    in_sum_features (GeoDataFrame): The features to summarize (in CRS 3310)
    group_field (str): Field to group by (default 'WHR13NAME')
    
    Returns:
    tuple: (summarized GeoDataFrame, summary table DataFrame)
    """
    # Verify CRS is 3310
    if in_polygons.crs.to_epsg() != 3310 or in_sum_features.crs.to_epsg() != 3310:
        raise ValueError("Both input geodataframes must be in CRS 3310 (California Albers)")
    
    # Reset index of input polygons and keep the index as a column
    in_polygons = in_polygons.reset_index(drop=True)
    in_polygons['Join_ID'] = in_polygons.index
    
    # Perform spatial join

    # Create a bounding box of `in_polygons`
    bbox = in_polygons.total_bounds  # [minx, miny, maxx, maxy]

    logger.info(f"            enrich step 2/32 filter board veg types using the bounding box")
    # Filter `in_sum_features` using the bounding box
    in_sum_features_filtered = in_sum_features.cx[
        bbox[0]:bbox[2], bbox[1]:bbox[3]
    ]

    logger.info(f"               original board veg type records: {in_sum_features.shape[0]} ")
    logger.info(f"               filtered board veg type records: {in_sum_features_filtered.shape[0]} ")
    logger.info(f"               records for summary: {in_polygons.shape[0]}")
    
    logger.info(f"            enrich step 3/32 determining board veg types")
    in_polygons_updated = process_spatial_join_parallel(in_polygons, in_sum_features_filtered)
    logger.info(f"               assigning vegetation types is completed")
    show_columns(logger, in_polygons_updated, "in_polygons_updated")
    logger.debug('-'*70)
    logger.debug(in_polygons_updated[['Join_ID', 'BROAD_VEGETATION_TYPE', 'sum_Area_ACRES', 'Polygon_Count']])

    return in_polygons_updated


def enrich_polygons(enrich_in, a_reference_gdb_path, start_year, end_year):

    pd.options.display.float_format = '{:.15f}'.format
    
    logger.info(f"      Executing Polygon Enrichments...")

    # --------------------------------------------------
    logger.info(f"         Calculating Broad Vegetation Type...")

    # Load Broad_Vegetation_Types as GeoDataFrame
    logger.info(f"            enrich step 1/32 summarize veg within polygons")    
    start = time.time()
    
    if not os.path.exists("cache"):
        os.makedirs("cache")
        
    if os.path.exists("cache/Broad_Vegetation_Types.parquet"):
        veg_layer = gpd.read_parquet("cache/Broad_Vegetation_Types.parquet")
        logger.info("               Loaded Broad_Vegetation_Types from cache")
    else:
        veg_layer = gpd.read_file(a_reference_gdb_path, driver="OpenFileGDB", layer='Broad_Vegetation_Types')
        veg_layer.to_parquet("cache/Broad_Vegetation_Types.parquet")
        logger.info("               Loaded Broad_Vegetation_Types from source and cached")

    logger.info(f"                  time for loading Broad_Vegetation_Types: {time.time()-start}")

    logger.debug(f"{'-'*70}")
    logger.debug(f"veg_layer: {veg_layer.shape}  :  {list(veg_layer.columns)}")
    
    start = time.time()
    enrich_input_with_bvt = enrich_in[enrich_in['BROAD_VEGETATION_TYPE'].notna()]
    enrich_input_without_values = enrich_in[enrich_in['BROAD_VEGETATION_TYPE'].isna()]
    enrich_in = enrich_input_without_values.copy()
    
    trunk_size = 10000
    if enrich_in.shape[0] > trunk_size:
        estimated_time = int(enrich_in.shape[0] / 10000 * 60)
        logger.info(
            f"                  "
            f"Summarizing veg types with {enrich_in.shape[0]} records "
            f"may take up to {estimated_time} minutes depending on the geometries."
        )
        chunks = split_gdf(enrich_in, trunk_size)
        logger.info(f"               split into {len(chunks)} chunks with {trunk_size} records")
        enriched_list = []
        for index, chunk in enumerate(chunks):
            logger.info(f"            ================ processing chuck {index+1} ================")
            hash = hash_geodataframe(chunk)
            if os.path.exists(f"cache/{hash}.parquet"):
                enriched_chunk = gpd.read_parquet(f"cache/{hash}.parquet")
                logger.info(f"            loaded enriched chunks from the cache")
            else:
                enriched_chunk = summarize_within(chunk, veg_layer)
                enriched_chunk.to_parquet(f"cache/{hash}.parquet")
                logger.info(f"               saved enriched chunks into the cache")
            enriched_list.append(enriched_chunk)

        # Concatenate the list into a single GeoDataFrame
        veg_enriched = gpd.GeoDataFrame(pd.concat(enriched_list, ignore_index=True))

        # Ensure the geometry column is preserved as GeoSeries
        veg_enriched.set_geometry('geometry', inplace=True)

    else:
        veg_enriched = summarize_within(enrich_in, veg_layer)
    logger.info(f"               time for summarizing veg types: {time.time()-start}")

    veg_enriched = pd.concat([enrich_input_with_bvt, veg_enriched], ignore_index=True)
    
    logger.debug(f"{'-'*70}")
    logger.debug(f"veg_enriched:  {veg_enriched.shape}  :  {list(veg_enriched.columns)}")

    logger.info("            enrich step 7/32 select records where BROAD_VEGETATION_TYPE is not null")    
    mask_not_null = veg_enriched['BROAD_VEGETATION_TYPE'].notna()

    logger.info("            enrich step 8/32 set BVT_USERD of the selected records to YES")    
    veg_enriched.loc[mask_not_null, 'BVT_USERD'] = 'YES'

    logger.info("            enrich step 9/32 select records where BROAD_VEGETATION_TYPE is null")    
    mask_null = veg_enriched['BROAD_VEGETATION_TYPE'].isna()

    logger.info("            enrich step 11/32 set BVT_USERD of the selected records to NO")
    veg_enriched.loc[mask_null, 'BVT_USERD'] = 'NO'

    logger.debug(f"{'-'*70}")
    logger.debug(f"veg_enriched:\n {veg_enriched[['Join_ID', 'BROAD_VEGETATION_TYPE', 'BVT_USERD']]}")

    logger.info("            enrich step 12/32 keeping only the necessary columns")
    veg_enriched = veg_enriched.drop(columns=['Join_ID', 'sum_Area_ACRES', 'Polygon_Count'])
    
    logger.debug(f"{'-'*70}")
    logger.debug(f"veg_enriched: {veg_enriched.shape}  :  {veg_enriched.columns.to_list()}")
 
    del veg_layer

    # --------------------------------------------------
    logger.info(f"         Calculating WUI...")

    wui_input_gdf = veg_enriched

    # Load WUL as GeoDataFrame
    start = time.time()
    if os.path.exists("cache/WUI.parquet"):
        wui_layer = gpd.read_parquet("cache/WUI.parquet")
        logger.info("            Loaded WUI from cache")
    else:
        wui_layer = gpd.read_file(a_reference_gdb_path, driver="OpenFileGDB", layer='WUI')
        wui_layer.to_parquet("cache/WUI.parquet")
        logger.info("            Loaded WUI from source and cached")
    logger.info(f"               time for loading WUI: {time.time()-start}")

    logger.debug(f"{'-'*70}")
    logger.debug(f"wui_layer: {wui_layer.shape}  :  {list(wui_layer.columns)}")
    
    logger.info("            enrich step 13/32 select records with null WUI")
    null_wui_mask = wui_input_gdf['IN_WUI'].isna() | (wui_input_gdf['IN_WUI'] == '')
    wui_selected_gdf = wui_input_gdf[null_wui_mask].copy()

    logger.info("            enrich step 14/32 select by WUI location")
    wui_intersecting_gdf = gpd.sjoin(wui_selected_gdf, wui_layer, how='inner', predicate='intersects')
    
    logger.info("            enrich step 15/32 calculate WUI yes")
    wui_input_gdf.loc[wui_intersecting_gdf.index, 'IN_WUI'] = 'WUI_AUTO_POP'
    
    logger.info("            enrich step 16/32 select remaining null records")
    null_wui_mask = wui_input_gdf['IN_WUI'].isna() | (wui_input_gdf['IN_WUI'] == '')
    non_wui_gdf = wui_input_gdf[null_wui_mask].copy()

    logger.info("            enrich step 17/32 calculate WUI no")
    wui_input_gdf.loc[non_wui_gdf.index, 'IN_WUI'] = 'NON-WUI_AUTO_POP'
    show_columns(logger, wui_input_gdf, "wui_input_gdf")
    
    logger.info("            enrich step 18/32 feature to point")
    wui_centroids_gdf = wui_input_gdf.copy()
    wui_centroids_gdf['geometry'] = wui_input_gdf.geometry.centroid

    logger.info("            enrich step 19/32 setup ORIG_FID")
    wui_centroids_gdf['ORIG_FID'] = wui_input_gdf.index
    # wui_centroids_gdf is a point-based geodataframe
    show_columns(logger, wui_centroids_gdf, "wui_centroids_gdf")

    # --------------------------------------------------
    logger.info("         Calculating Ownership, Counties, and Regions...")

    # Load CALFIRE_Ownership_Update as GeoDataFrame
    start = time.time()
    if os.path.exists("cache/CALFIRE_Ownership_Update.parquet"):
        ownership_layer = gpd.read_parquet("cache/CALFIRE_Ownership_Update.parquet")
        logger.info("            Loaded CALFIRE_Ownership_Update from cache")
    else:
        ownership_layer = gpd.read_file(a_reference_gdb_path, driver="OpenFileGDB", layer='CALFIRE_Ownership_Update')
        ownership_layer.to_parquet("cache/CALFIRE_Ownership_Update.parquet")
        logger.info("            Loaded CALFIRE_Ownership_Update from source and cached")
    logger.info(f"               time for loading CALFIRE_Ownership_Update: {time.time()-start}")

    show_columns(logger, ownership_layer, "ownership_layer")
    
    # Load WFRTF_Regions as GeoDataFrame
    start = time.time()
    if os.path.exists("cache/WFRTF_Regions.parquet"):
        regions_layer = gpd.read_parquet("cache/WFRTF_Regions.parquet")
        logger.info("            Loaded WFRTF_Regions from cache")
    else:
        regions_layer = gpd.read_file(a_reference_gdb_path, driver="OpenFileGDB", layer='WFRTF_Regions')
        regions_layer.to_parquet("cache/WFRTF_Regions.parquet")
        logger.info("            Loaded WFRTF_Regions from source and cached")
    logger.info(f"               time for loading WFRTF_Regions: {time.time()-start}")

    show_columns(logger, regions_layer, "regions_layer")

    logger.info("            enrich step 20/32 spatial join ownership")    
    wui_centroids_ownership = gpd.sjoin(wui_centroids_gdf, ownership_layer, how="left", predicate="intersects")
    wui_centroids_ownership = wui_centroids_ownership.drop(columns=['GIS_ACRES', 'Shape_Length', 'Shape_Area'])
    if "index_right" in wui_centroids_ownership.columns:
        wui_centroids_ownership = wui_centroids_ownership.drop(columns=["index_right"])
    
    logger.debug(f"{'-'*70}")
    logger.debug(f"wui_centroids_ownership: {wui_centroids_ownership.shape}  :  {list(wui_centroids_ownership.columns)}")

    logger.info("            enrich step 21/32 spatial join with regions layer")
    wui_centroids_ownership_regions = gpd.sjoin(wui_centroids_ownership, regions_layer, how="left", predicate="intersects")
    wui_centroids_ownership_regions = wui_centroids_ownership_regions.drop(columns=['GIS_Acres', 'Shape_Area', 'Shape_Length'])
    if "index_right" in wui_centroids_ownership_regions.columns:
        wui_centroids_ownership_regions = wui_centroids_ownership_regions.drop(columns=["index_right"])

    logger.debug(f"{'-'*70}")
    logger.debug(f"wui_centroids_ownership_regions: {wui_centroids_ownership_regions.shape}  :  {list(wui_centroids_ownership_regions.columns)}")
    
    logger.info("            enrich step 22/32 add ownership and region")
    # join_3 = select_7.merge(wui_centroids_ownership_regions, left_on="OBJECTID", right_on="ORIG_FID", how="left")

    # Drop all columns in wui_input_gdf from wui_centroids_ownership_regions
    columns_to_keep = wui_centroids_ownership_regions.columns.difference(wui_input_gdf.columns).tolist()

    # Subset wui_centroids_ownership_regions to only keep these columns
    subset_gdf = wui_centroids_ownership_regions[columns_to_keep]

    # Merge the subset back into the original wui_input_gdf
    wui_centroids_ownership_regions = wui_input_gdf.merge(
        subset_gdf,
        left_index=True,
        right_on='ORIG_FID',
        how='left'
    )
    
    logger.info("            enrich step 23/32 calculate ownership field")
    wui_centroids_ownership_regions['PRIMARY_OWNERSHIP_GROUP'] = wui_centroids_ownership_regions['AGNCY_LEV']

    logger.info("            enrich step 24/32 calculate county field")
    wui_centroids_ownership_regions['COUNTY'] = wui_centroids_ownership_regions['COUNTY_right']

    logger.info("            enrich step 25/32 calculate region field")
    wui_centroids_ownership_regions['REGION'] = wui_centroids_ownership_regions['Region']

    logger.info("            enrich step 26/32 set TRMT_GEOM")
    wui_centroids_ownership_regions['TRMT_GEOM'] = 'POLYGON'

    logger.info("            enrich step 27/32 calculating years...")
    calculate_fiscal_years(wui_centroids_ownership_regions) 
    
    logger.info("            enrich step 28/32 Initiating Crosswalk...")
    crosswalk_table = crosswalk(wui_centroids_ownership_regions, a_reference_gdb_path, start_year, end_year)  
    
    logger.info("         Crosswalk Complete. Continuing Enrichment...")

    logger.info("            enrich step 29/32 Calculating Latitude and Longitude...")
    centroids_albers = crosswalk_table.geometry.centroid

    # Create a new GeoDataFrame with just the centroids and project back to WGS84
    centroid_gdf = gpd.GeoDataFrame(geometry=centroids_albers, crs=crosswalk_table.crs)
    centroid_gdf_wgs84 = centroid_gdf.to_crs(epsg=4269)
    
    # Extract coordinates from the reprojected centroids
    crosswalk_table['LATITUDE'] = centroid_gdf_wgs84.geometry.y
    crosswalk_table['LONGITUDE'] = centroid_gdf_wgs84.geometry.x
        
    logger.info("            enrich step 30/32 calculate treatment acres")
    crosswalk_table['TREATMENT_AREA'] = crosswalk_table.geometry.area * 0.000247105
    
    logger.info("            enrich step 31/32 removing unnecessary fields")
    enriched_gdf = keep_fields(crosswalk_table)
    show_columns(logger, enriched_gdf, "enriched_gdf")
    
    logger.info("            enrich step 32/32 delete if County is Null")
    enriched_gdf = enriched_gdf[enriched_gdf["COUNTY"].notnull()]
    show_columns(logger, enriched_gdf, "enriched_gdf")
    
    return enriched_gdf
