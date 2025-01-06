
import hashlib
import logging

import pandas as pd
import geopandas as gpd
import shapely

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


def hash_geodataframe(gdf):
    # Make a copy of the GeoDataFrame to avoid modifying the original
    temp_gdf = gdf.copy()

    # Sort the GeoDataFrame for consistent hashing
    temp_gdf = temp_gdf.sort_index(axis=0).sort_index(axis=1)

    # Temporarily create a WKT column for hashing purposes (without modifying original 'geometry')
    temp_gdf['geometry_wkt'] = temp_gdf['geometry'].apply(
        lambda geom: geom.wkt if isinstance(geom, shapely.geometry.base.BaseGeometry) else None
    )

    # Convert the DataFrame to CSV string for hashing, excluding original 'geometry' column
    df_string = temp_gdf.drop(columns=['geometry']).to_csv(index=False)

    # Hash the CSV string
    hash_object = hashlib.sha256(df_string.encode('utf-8'))

    return hash_object.hexdigest()


def capitalize_columns(gdf):
    # Create a dictionary to rename columns
    rename_dict = {col: col.upper() for col in gdf.columns if col != 'geometry'}

    rename_dict['globalid'] = 'GlobalID'
    rename_dict['Shape_Area'] = 'Shape_Area'
    rename_dict['Shape_Length'] = 'Shape_Length'
    rename_dict['geometry'] = 'geometry'
    rename_dict['globalid_text'] = 'GlobalID_text'
    rename_dict['org_admin_p'] = 'ORG_ADMIN_p'
    rename_dict['org_admin_t'] = 'ORG_ADMIN_t'
    rename_dict['org_admin_a'] = 'ORG_ADMIN_a'
    
    # Apply the renaming
    return gdf.rename(columns=rename_dict)


def get_rows_with_empty_geometry(gdf):
    gdf_na = gdf[gdf.geometry.isna() | gdf.geometry.is_empty | gdf['geometry'].isnull()]
    return gdf_na.shape
