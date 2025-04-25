
import logging
import os
import pandas as pd
import geopandas as gpd
import dask_geopandas

from multiprocessing import Pool

import sys
sys.path.append('../')

from its_logging.logger_config import logger
from utils.gdf_utils import get_rows_with_empty_geometry
from utils.save_gdf_to_gdb import save_gdf_to_gdb


logger = logging.getLogger('process.append_polygon')



def append_enriched_features(layers):
    gdfs_to_append = []
    for layer in layers:
        logger.info(f"Load GeoDataFrame from the layer '{layer['layer_name']}' in '{layer['gdb_path']}' ")
        gdf = gpd.read_file(layer['gdb_path'], driver="OpenFileGDB", layer=layer['layer_name'])
        if gdf.crs != "EPSG:3310":
            gdf = gdf.to_crs("EPSG:3310")
        

        if get_rows_with_empty_geometry(gdf)[0] > 0:
            
            logger.info("Found empty geometry in the data in " + layer['layer_name'])
            gdf_na = gdf[gdf.geometry.isna() | gdf.geometry.is_empty | gdf['geometry'].isnull()]
            # only COUNTS_TO_MAS == NO data exist, remove and continue
            if len(gdf_na['COUNTS_TO_MAS'].unique()) == 1 and gdf_na['COUNTS_TO_MAS'].unique()[0] == 'NO':
                logger.info("Only COUNTS_TO_MAS == NO, remove empty geometry in the data")
                gdf = gdf.drop(gdf_na.index)
            # found data error
            else:
                logger.info("Only COUNTS_TO_MAS == YES, error")
                exit()
        gdfs_to_append.append(gdf)
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


if __name__ == '__main__':
    # TODO: temporary file path
    enriched_path = r"D:\WORK\wildfire\Interagency-Tracking-System\its\ITSGDB_backup\tmp"
    enriched_list = os.listdir(enriched_path)

    point_layers = []
    line_layers = []
    poly_layers = []
    for f in enriched_list:
        f_path = os.path.join(enriched_path, f)
        gdb_layers = gpd.list_layers(f_path)
        for i in range(len(gdb_layers)):
            if 'point' in gdb_layers.loc[i, 'geometry_type'].lower():
                point_layers.append({'gdb_path': f_path, 'layer_name':gdb_layers.loc[i, 'name']})
            elif 'line' in gdb_layers.loc[i, 'geometry_type'].lower():
                line_layers.append({'gdb_path': f_path, 'layer_name':gdb_layers.loc[i, 'name']})
            else:
                poly_layers.append({'gdb_path': f_path, 'layer_name':gdb_layers.loc[i, 'name']})
                
    enriched_layers = {'point': point_layers, 
                    'line': line_layers,
                    'polygon': poly_layers}
    
    # TODO: temporary file path
    # add timber spatial from V1.1 GDB
    enriched_layers['polygon'].append({'gdb_path': r"D:\WORK\wildfire\Interagency-Tracking-System\its\Interagency Tracking System.gdb", 'layer_name': 'Timber_Industry_Spatial_20241130'})

    enriched_polygons, enriched_lines, enriched_points = get_enriched_features(enriched_layers)

    # TODO: temporary file path
    california_boundary = gpd.read_file(r'D:\WORK\wildfire\Interagency-Tracking-System\its\Interagency Tracking System.gdb', 
                                    driver='OpenFileGDB', 
                                    layer='California')
    

    append_path = r"D:\WORK\wildfire\Interagency-Tracking-System\its\ITSGDB_backup\V2.0\appended.gdb"

    for df, lyr_name in zip([enriched_polygons,enriched_lines,enriched_points], ["appended_poly","appended_line","appended_point"]):
        # init dask gdf for multithread clipping
        ddf = dask_geopandas.from_geopandas(df, npartitions=16)
        # clip to california bounds
        append_clipped = ddf.sjoin(california_boundary, how='inner', predicate='intersects').compute()
        save_gdf_to_gdb(append_clipped, append_path, lyr_name)
