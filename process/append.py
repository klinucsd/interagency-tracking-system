
import logging
import os
import pandas as pd
import geopandas as gpd
import dask_geopandas
import yaml
import argparse


from multiprocessing import Pool

import sys
sys.path.append('../')

from its_logging.logger_config import logger
from utils.gdf_utils import get_rows_with_empty_geometry
from utils.save_gdf_to_gdb import save_gdf_to_gdb
from utils.category import categorize_activity
from utils.standardize_domains import standardize_domains
from utils.counts_to_mas import counts_to_mas



logger = logging.getLogger('process.append_polygon')

def check_core_criteria(row):
    core_eval = 0
    if row['ADMINISTERING_ORG'] is not None:
        core_eval +=1
    # status complete
    if row['ACTIVITY_STATUS'] == 'COMPLETE':
        core_eval +=1
        # if complete must have a not none end date
        if row['ACTIVITY_END'] is not None:
            core_eval +=1
    # if status is not complete but filled 
    # activity_end does not need to be valid
    elif row['ACTIVITY_STATUS'] is not None:
        core_eval +=2
        
    if row['ACTIVITY_QUANTITY'] > 0:
        core_eval +=1
    
    if row['ACTIVITY_UOM'] is not None:
        core_eval +=1
        
    return core_eval

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


def get_all_enriched_paths(enriched_path):

     # enumerate all gdb file paths in the input folder path
    enriched_list = os.listdir(enriched_path)

    # skip the append, reports gdb file path if they are stored in the same folder
    skip_list = ['appended.gdb', 'reports.gdb']

    point_layers = []
    line_layers = []
    poly_layers = []
    for f in enriched_list:
        if f in skip_list:
            continue
        f_path = enriched_path + r"\{}".format(f)
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
    
    return enriched_layers

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

    with open("..\config.yaml", 'r') as stream:
        config_inputs = yaml.safe_load(stream)


    enriched_path = config_inputs['appended']['input_path']
    reference_path = config_inputs['global']['reference_gdb']
    california_boundary_layer_name = config_inputs['global']['california_boundry_layer_name']
    output_append_path = config_inputs['appended']['gdb_path']
    start_year = config_inputs['global']['start_year']
    end_year = config_inputs['global']['end_year']

    enriched_layers = get_all_enriched_paths(enriched_path)

    # read command line argument
    parser = argparse.ArgumentParser(description='Append enriched files. Optional arguement: "--geom_type"; valid input: ["point", "line", "polygon", "all"]; default to "all" ')
    parser.add_argument("--geom_type", type=str, default="all")
    args = parser.parse_args()
    # if only a specific geom_type needs to be processed
    if args.geom_type == "point":
        enriched_layers = {'point': enriched_layers['point'], 
                    'line': [],
                    'polygon': []}
    elif args.geom_type == "line":
        enriched_layers = {'point': [], 
                    'line': enriched_layers['line'],
                    'polygon': []}
    elif args.geom_type == "polygon":
        enriched_layers = {'point': [], 
                    'line': [],
                    'polygon': enriched_layers['polygon']}
    elif args.geom_type == "all":
        pass
    else:
        raise ValueError('Input must be from ["point", "line", "polygon", "all"].')
    
    # force reapply domain standardization for sanity
    for lyr_type in enriched_layers.keys():
        for path_dict in enriched_layers[lyr_type]:
            gdf = gpd.read_file(path_dict['gdb_path'], driver='OpenFileGDB', layer=path_dict['layer_name'])
            gdf = counts_to_mas(standardize_domains(categorize_activity(gdf)), start_year, end_year)
            gdf.to_file(path_dict['gdb_path'], driver='OpenFileGDB', layer=path_dict['layer_name'])
    
    # read enriched geodatabase to geopandas gdf and concat together
    enriched_polygons, enriched_lines, enriched_points = get_enriched_features(enriched_layers)

    # read california boundary for cliping
    california_boundary = gpd.read_file(reference_path, 
                                    driver='OpenFileGDB', 
                                    layer=california_boundary_layer_name)
    
    # grab timber non spatial path again
    timber_nonspatial_path = None
    timber_nonspatial = None
    for p in enriched_layers['point']:
        if 'Timber_Nonspatial' in p['gdb_path']:
            timber_nonspatial_path = p
            break
    if timber_nonspatial_path:
        timber_nonspatial = gpd.read_file(timber_nonspatial_path['gdb_path'], 
                                    driver='OpenFileGDB', 
                                    layer=timber_nonspatial_path['layer_name'])
    

    # use dask geopandas for multithread clipping
    for df, lyr_name in zip([enriched_polygons,enriched_lines,enriched_points], ["appended_poly","appended_line","appended_point"]):
        # init dask gdf for multithread clipping
        ddf = dask_geopandas.from_geopandas(df, npartitions=16)
        # clip to california bounds
        append_clipped = ddf.sjoin(california_boundary, how='inner', predicate='intersects').compute()
        # drop unwanted artifact columns from California boundary df
        append_clipped = append_clipped.drop(['index_right', 'Shape_Area', 'Shape_Length'], axis=1)
        
        # industry nonspatial is by design out of california bounds and got clipped, manually concat it back
        if lyr_name == 'appended_point':
            append_clipped = pd.concat([append_clipped, timber_nonspatial], ignore_index=True)

        append_clipped['CORE_CRITERIA'] = append_clipped.apply(check_core_criteria, axis=1)
        save_gdf_to_gdb(append_clipped, output_append_path, lyr_name)