
import os
import time
import logging
import pandas as pd
import geopandas as gpd
from datetime import datetime
import numpy as np

from its_logging.logger_config import logger
from utils.gdf_utils import show_columns
from utils.keep_fields import keep_fields
from utils.year import calculate_fiscal_years
from utils.crosswalk import crosswalk


logger = logging.getLogger('utils.enrich_points')


def enrich_points(points_gdf, a_reference_gdb_path, start_year, end_year):

    pd.options.display.float_format = '{:.15f}'.format
    
    logger.info(f"      Executing Point Enrichments...")
    
    # Create a copy of the input points
    wui_input_gdf = points_gdf.copy()
    show_columns(logger, wui_input_gdf, "wui_input_gdf")

    # --------------------------------------------------
    logger.info(f"         Calculating WUI...")

    if not os.path.exists("cache"):
        os.makedirs("cache")
    
    # Load WUL as GeoDataFrame
    start = time.time()
    if os.path.exists("cache/WUI.parquet"):
        logger.info("            enrich step 1/16 loading WUI from cache")
        wui_layer = gpd.read_parquet("cache/WUI.parquet")
    else:
        logger.info("            enrich step 1/16 loading WUI from source and cache the data")
        wui_layer = gpd.read_file(a_reference_gdb_path, driver="OpenFileGDB", layer='WUI')
        wui_layer.to_parquet("cache/WUI.parquet")
    logger.info(f"               time for loading WUI: {time.time()-start}")

    logger.debug(f"{'-'*70}")
    logger.debug(f"wui_layer: {wui_layer.shape}  :  {list(wui_layer.columns)}")

    # --------------------------------------------------------------
    # WUI Processing

    # ----- delete the following line 
    wui_input_gdf["IN_WUI"] = None

    logger.info("            enrich step 2/16 select records with null WUI")
    null_wui_mask = wui_input_gdf['IN_WUI'].isna() | (wui_input_gdf['IN_WUI'] == '')
    wui_selected_gdf = wui_input_gdf[null_wui_mask].copy()

    logger.info("            enrich step 3/16 select by WUI location")
    wui_intersecting_gdf = gpd.sjoin(wui_selected_gdf, wui_layer, how='inner', predicate='intersects')
    
    logger.info("            enrich step 4/16 calculate WUI yes")
    wui_input_gdf.loc[wui_intersecting_gdf.index, 'IN_WUI'] = 'WUI_AUTO_POP'
    
    logger.info("            enrich step 5/16 select remaining null records")
    null_wui_mask = wui_input_gdf['IN_WUI'].isna() | (wui_input_gdf['IN_WUI'] == '')
    non_wui_gdf = wui_input_gdf[null_wui_mask].copy()

    logger.info("            enrich step 6/16 calculate WUI no")
    wui_input_gdf.loc[non_wui_gdf.index, 'IN_WUI'] = 'NON-WUI_AUTO_POP'
    show_columns(logger, wui_input_gdf, "wui_input_gdf")


    #-------------------------------------------------------------------
    logger.info("         Calculating Ownership, Counties, and Regions...")
    
    # Load CALFIRE_Ownership_Update as GeoDataFrame
    start = time.time()
    if os.path.exists("cache/CALFIRE_Ownership_Update.parquet"):
        logger.info("            enrich step 7/16 loading CALFIRE_Ownership_Update from cache")
        ownership_gdf = gpd.read_parquet("cache/CALFIRE_Ownership_Update.parquet")
    else:
        logger.info("            enrich step 7/16 loading CALFIRE_Ownership_Update from source and cache the data")
        ownership_gdf = gpd.read_file(a_reference_gdb_path, driver="OpenFileGDB", layer='CALFIRE_Ownership_Update')
        ownership_gdf.to_parquet("cache/CALFIRE_Ownership_Update.parquet")
    logger.info(f"               time for loading CALFIRE_Ownership_Update: {time.time()-start}")

    ownership_gdf = ownership_gdf.drop(columns=['GIS_ACRES', 'Shape_Area', 'Shape_Length'])
    show_columns(logger, ownership_gdf, "ownership_gdf")

    
    # Ownership Processing
    logger.info("            enrich step 8/16 spatial join ownership")
    ownership_wui_gdf = gpd.sjoin_nearest(
        wui_input_gdf,
        ownership_gdf,
        how='left'
    )
    ownership_wui_gdf['PRIMARY_OWNERSHIP_GROUP'] = ownership_wui_gdf['AGNCY_LEV']
    ownership_wui_gdf = ownership_wui_gdf.drop(columns=['index_right'])
    show_columns(logger, ownership_wui_gdf, "ownership_wui_gdf")
    
    #-----------------------------------------------------------------------    
    # Regions Processing

    start = time.time()
    if os.path.exists("cache/WFRTF_Regions.parquet"):
        logger.info("            enrich step 9/16 loading WFRTF_Regions from cache")
        regions_gdf = gpd.read_parquet("cache/WFRTF_Regions.parquet")
    else:
        logger.info("            enrich step 9/16 loading WFRTF_Regions from source and cache the data")
        regions_gdf = gpd.read_file(a_reference_gdb_path, driver="OpenFileGDB", layer='WFRTF_Regions')
        regions_gdf.to_parquet("cache/WFRTF_Regions.parquet")
    logger.info(f"               time for loading WFRTF_Regions: {time.time()-start}")

    show_columns(logger, regions_gdf, "regions_gdf")

    logger.info("            enrich step 10/16 spatial join regions")
    regions_ownership_wui_gdf = gpd.sjoin_nearest(
        ownership_wui_gdf,
        regions_gdf,
        how='left'
    )
    show_columns(logger, regions_ownership_wui_gdf, "regions_ownership_wui_gdf")
    
    regions_ownership_wui_gdf['REGION'] = regions_ownership_wui_gdf['Region']
    regions_ownership_wui_gdf['COUNTY'] = regions_ownership_wui_gdf['COUNTY_right']
    
    # --------------------------------------------------------------------------    
    # Vegetation Processing
        
    if os.path.exists("cache/Broad_Vegetation_Types.parquet"):
        logger.info("            enrich step 11/16 loading Broad_Vegetation_Types from cache")
        veg_layer = gpd.read_parquet("cache/Broad_Vegetation_Types.parquet")
    else:
        logger.info("            enrich step 11/16 loading Broad_Vegetation_Types from source and cached")
        veg_layer = gpd.read_file(a_reference_gdb_path, driver="OpenFileGDB", layer='Broad_Vegetation_Types')
        veg_layer.to_parquet("cache/Broad_Vegetation_Types.parquet")
    logger.info(f"               time for loading Broad_Vegetation_Types: {time.time()-start}")

    logger.debug(f"{'-'*70}")
    logger.debug(f"veg_layer: {veg_layer.shape}  :  {list(veg_layer.columns)}")

    logger.info("            enrich step 12/16 spatial join veg and calculations")
    regions_ownership_wui_gdf = regions_ownership_wui_gdf.drop(columns=['index_right'])
    veg_regions_ownership_wui_gdf = gpd.sjoin_nearest(
        regions_ownership_wui_gdf,
        veg_layer,
        how='left'
    )

    show_columns(logger, veg_regions_ownership_wui_gdf, "veg_regions_ownership_wui_gdf")
    
    # Only update vegetation type if it's not already set
    mask = veg_regions_ownership_wui_gdf['BROAD_VEGETATION_TYPE'].isna()
    veg_regions_ownership_wui_gdf.loc[mask, 'BROAD_VEGETATION_TYPE'] = veg_regions_ownership_wui_gdf.loc[mask, 'WHR13NAME']
    veg_regions_ownership_wui_gdf['BVT_USERD'] = 'NO'


    # -------------------------------------------------------------
    logger.info("            enrich step 13/16 Initiating Crosswalk")

    points_enriched = crosswalk(veg_regions_ownership_wui_gdf, a_reference_gdb_path, start_year, end_year)      
    
    logger.info("         Crosswalk Complete. Continuing Enrichment...")
    
    
    logger.info("            enrich step 14/16 calculating Years")
    points_enriched = calculate_fiscal_years(points_enriched) 

    
    logger.info("            enrich step 15/16 calculating Latitude and Longitude")
    # Extract coordinates from geometry
    points_enriched['LATITUDE'] = points_enriched.geometry.y
    points_enriched['LONGITUDE'] = points_enriched.geometry.x
    points_enriched['TRMT_GEOM'] = 'POINT'
    
    logger.info("            enrich step 16/16 removing unnecessary fields")
    points_enriched = keep_fields(points_enriched)

    logger.info("         Enrich Points Complete...")
    
    return points_enriched
