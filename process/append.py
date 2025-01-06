
import logging
import pandas as pd
import geopandas as gpd

from its_logging.logger_config import logger
from utils.gdf_utils import get_rows_with_empty_geometry


logger = logging.getLogger('process.append_polygon')


def append_enriched_features(layers):
    gdfs_to_append = []
    for layer in layers:
        logger.info(f"Load GeoDataFrame from the layer '{layer['layer_name']}' in '{layer['gdb_path']}' ")
        gdf = gpd.read_file(layer['gdb_path'], driver="OpenFileGDB", layer=layer['layer_name'])
        if gdf.crs != "EPSG:3310":
            gdf = gdf.to_crs("EPSG:3310")
        gdfs_to_append.append(gdf)

        if get_rows_with_empty_geometry(gdf)[0] > 0:
            logger.error("Found empty geometry in the data")
            exit()

    if gdfs_to_append:
        final_gdf = pd.concat(gdfs_to_append, ignore_index=True)
        logger.info(f"Concatenated all geodataframes and got {final_gdf.shape[0]} records")
    else:
        final_gdf = None
    
    return final_gdf


def get_enriched_features(enriched_data_spec):

    # Concatenate enriched points, lines and polygons                                                                                                                         
    logger.info("-"*80)
    logger.info("Concatenate all polygon records")
    enriched_polygons = append_enriched_features(enriched_data_spec['polygon'])

    logger.info("-"*80)
    logger.info("Concatenate all line records")
    enriched_lines = append_enriched_features(enriched_data_spec['line'])

    logger.info("-"*80)
    logger.info("Concatenate all point records")
    enriched_points = append_enriched_features(enriched_data_spec['point'])
    
    return enriched_polygons, enriched_lines, enriched_points
