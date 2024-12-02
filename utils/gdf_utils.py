

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
