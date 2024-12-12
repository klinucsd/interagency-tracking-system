
import os
import time
import logging
import pandas as pd
import geopandas as gpd
from datetime import datetime

from its_logging.logger_config import logger
from utils.gdf_utils import show_columns
from utils.keep_fields import keep_fields
from utils.year import calculate_fiscal_years
from utils.crosswalk import crosswalk


logger = logging.getLogger('utils.enrich_polygons')


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
    joined = gpd.sjoin(in_sum_features, in_polygons, how='right', predicate='intersects')
    
    # Calculate area in acres
    # CRS 3310 uses meters, so convert square meters to acres
    joined['area_acres'] = joined.geometry.area * 0.000247105
    
    # Add a count column
    joined['count'] = 1
    
    # Summarize by input polygons
    summary = joined.groupby(['Join_ID', group_field]).agg({
        'area_acres': 'sum',
        'count': 'sum'
    }).reset_index()
    
    # Rename columns to match expected output
    summary.columns = ['Join_ID', group_field, 'sum_Area_ACRES', 'Polygon_Count']
    
    # Create the output feature class
    result = in_polygons.copy()
    result['sum_Area_ACRES'] = 0  # Initialize area field
    result['Polygon_Count'] = 0   # Initialize count field
    
    # Update areas and counts in the result
    for idx, group in summary.groupby('Join_ID'):
        result.loc[idx, 'sum_Area_ACRES'] = group['sum_Area_ACRES'].sum()
        result.loc[idx, 'Polygon_Count'] = group['Polygon_Count'].sum()
    
    # Round the acre values to 2 decimal places for consistency with ArcGIS
    result['sum_Area_ACRES'] = result['sum_Area_ACRES']
    
    # Create the group table with specified columns
    group_table = summary.copy()
    
    # Round the acre values in group table
    group_table['sum_Area_ACRES'] = group_table['sum_Area_ACRES']
    
    # Ensure columns are in the correct order
    group_table = group_table[['Join_ID', 'WHR13NAME', 'sum_Area_ACRES', 'Polygon_Count']]
    
    return result, group_table


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
    veg_sum_gdf, veg_group_df = summarize_within(enrich_in, veg_layer)
    logger.info(f"                  time for summarizing: {time.time()-start}")

    logger.debug(f"{'-'*70}")
    logger.debug(f"veg_sum_gdf:  {veg_sum_gdf.shape}  :  {list(veg_sum_gdf.columns)}")

    logger.debug(f"{'-'*70}")
    logger.debug(f"veg_group_df: {veg_group_df.shape}  :  {list(veg_group_df.columns)}")
    logger.debug(f"veg_group_df:\n {veg_group_df.head()}")
    
    logger.info(f"            enrich step 2/32 summarize attributes")
    veg_max_sum_df = veg_group_df.groupby('Join_ID')['sum_Area_ACRES'].max().reset_index(name='MAX_Sum_Area_ACRES')

    logger.debug(f"{'-'*70}")
    logger.debug(f"veg_max_sum_df: {veg_max_sum_df.shape}  :  {list(veg_max_sum_df.columns)}")
    logger.debug(f"veg_max_sum_df:\n {veg_max_sum_df.head()}")
    
    logger.info(f"            enrich step 3/32 join sum_acres and max_sum_acres")
    veg_sum_max_joined_df = veg_max_sum_df.merge(
        veg_group_df,
        left_on=['Join_ID', 'MAX_Sum_Area_ACRES'],
        right_on=['Join_ID', 'sum_Area_ACRES'],
        how='left'
    )

    logger.debug(f"{'-'*70}")
    logger.debug(f"veg_sum_max_joined_df: {veg_sum_max_joined_df.shape}  :  {list(veg_sum_max_joined_df.columns)}")
    logger.debug(f"veg_sum_max_joined_df:\n {veg_sum_max_joined_df}")

    logger.info("            enrich step 4/32 decide broad vegetation types for identical max sum")
    veg_sum_max_final_df = veg_sum_max_joined_df.drop_duplicates(
        # subset=['Join_ID', 'MAX_Sum_Area_ACRES', 'WHR13NAME']
        subset=['Join_ID', 'MAX_Sum_Area_ACRES']
    )

    logger.info("            enrich step 5/32 keep Join_ID and WHR13NAME only")
    veg_sum_max_final_df = veg_sum_max_final_df[['Join_ID', 'WHR13NAME']]
    
    logger.debug(f"{'-'*70}")
    logger.debug(f"veg_sum_max_final_df: {veg_sum_max_final_df.shape}  :  {list(veg_sum_max_final_df.columns)}")
    logger.debug(f"veg_sum_max_final_df:\n {veg_sum_max_final_df}")

    logger.info("            enrich step 6/32 append broad vegetation types to veg_sum")    
    veg_sum_with_max_gdf = veg_sum_gdf.merge(
        veg_sum_max_final_df,
        on='Join_ID',
        how='left'
    )

    logger.debug(f"{'-'*70}")
    logger.debug(f"veg_sum_with_max_gdf: {veg_sum_with_max_gdf.shape}  :  {list(veg_sum_with_max_gdf.columns)}")
    logger.debug(f"veg_sum_with_max_gdf:\n {veg_sum_with_max_gdf}")

    logger.info("            enrich step 7/32 select records where BROAD_VEGETATION_TYPE is not null")    
    mask_not_null = veg_sum_with_max_gdf['BROAD_VEGETATION_TYPE'].notna()

    logger.info("            enrich step 8/32 set BVT_USERD of the selected records to YES")    
    veg_sum_with_max_gdf.loc[mask_not_null, 'BVT_USERD'] = 'YES'

    logger.info("            enrich step 9/32 select records where BROAD_VEGETATION_TYPE is null")    
    mask_null = veg_sum_with_max_gdf['BROAD_VEGETATION_TYPE'].isna()

    logger.info("            enrich step 10/32 update BROAD_VEGETATION_TYPE with WHR13NAME")    
    veg_sum_with_max_gdf.loc[mask_null, 'BROAD_VEGETATION_TYPE'] = veg_sum_with_max_gdf.loc[mask_null, 'WHR13NAME']

    logger.info("            enrich step 11/32 set BVT_USERD of the selected records to NO")
    veg_sum_with_max_gdf.loc[mask_null, 'BVT_USERD'] = 'NO'

    logger.debug(f"{'-'*70}")
    logger.debug(f"veg_sum_with_max_gdf:\n {veg_sum_with_max_gdf[['Join_ID', 'BROAD_VEGETATION_TYPE', 'BVT_USERD']]}")

    logger.info("            enrich step 12/32 keeping only the necessary columns")
    common_columns = set(veg_sum_with_max_gdf.columns).intersection(set(veg_group_df.columns))
    veg_sum_with_max_gdf = veg_sum_with_max_gdf.drop(columns=list(common_columns))
    
    logger.debug(f"{'='*70}")
    logger.debug(f"veg_sum_with_max_gdf: {veg_sum_with_max_gdf.shape}  :  {list(veg_sum_with_max_gdf.columns)}")
    
    del veg_layer

    # --------------------------------------------------
    logger.info(f"         Calculating WUI...")

    wui_input_gdf = veg_sum_with_max_gdf 

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
