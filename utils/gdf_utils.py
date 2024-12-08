
import pandas as pd
import geopandas as gpd

import logging
from its_logging.logger_config import logger


logger = logging.getLogger('utils.gdf_utils')


def verify_gdf_columns(gdf, required_columns, logger):
    """
    Verifies that the GeoDataFrame contains the required columns.
    
    Parameters:
        gdf (geopandas.GeoDataFrame): The GeoDataFrame to check.
        required_columns (list): A list of required column names.
    
    Raises:
        ValueError: If any required columns are missing.
    """
    # Get the missing columns
    missing_columns = [col for col in required_columns if col not in gdf.columns]
    
    if missing_columns:
        logger.error(f"   the following required columns are missing: {missing_columns}")
        exit()
    else:
        logger.info("   all required columns are present.")


def repair_geometries(gdf):

    # Check for invalid geometries in the GeoDataFrame
    gdf['is_valid'] = gdf.geometry.is_valid

    # Attempt to fix invalid geometries using the buffer(0) trick
    gdf['geometry'] = gdf.geometry.buffer(0)

    # Optionally, re-check for validity after fixing
    gdf['is_valid'] = gdf.geometry.is_valid

    # Remove the 'is_valid' column if it's no longer needed
    gdf = gdf.drop(columns=['is_valid'])

    return gdf


def show_columns(logger, gdf, name, sort=True):

    logger.debug("-"*70)
    tmp = list(gdf.columns)
    if sort:
        tmp.sort()
    logger.debug(f"{name} columns: {list(tmp)}")
    logger.debug(f"{name} dimension: {gdf.shape}")


def fetch_arcgis_feature_service(url, max_records=1000):
    offset = 0
    combined_gdf = None

    while True:
        query_url = f"{url}/query?where=1%3D1&outFields=*&outSR=3310&f=geojson&resultOffset={offset}&resultRecordCount={max_records}"
        logger.info(f"   loading {offset} - {offset + max_records} records")
        
        gdf = gpd.read_file(query_url)

        if gdf.empty:
            break

        # Convert float columns to integer
        for column in gdf.select_dtypes(include=['float64']).columns:
            gdf[column] = gdf[column].fillna(0).astype('int64')

        # Combine with previous results
        if combined_gdf is None:
            combined_gdf = gdf
        else:
            combined_gdf = pd.concat([combined_gdf, gdf], ignore_index=True)

        offset += max_records

        if len(gdf) < max_records:
            break

    logger.info(f"   total record count: {combined_gdf.shape[0]}")
        
    return combined_gdf
