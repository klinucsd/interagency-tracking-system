
import logging
import numpy as np
import multiprocessing as mp
import pandas as pd
import geopandas as gpd
from its_logging.logger_config import logger
from concurrent.futures import ProcessPoolExecutor
from functools import partial
import os
from pathlib import Path
import glob

logger = logging.getLogger('utils.concurrent_join')

def cleanup_parquet_files(pattern="/tmp/chunk_result_*"):
    """Clean up all temporary parquet files matching the pattern"""
    try:
        files = glob.glob(pattern)
        for f in files:
            try:
                os.remove(f)
                logger.debug(f"Removed temporary file: {f}")
            except Exception as e:
                logger.warning(f"Failed to remove temporary file {f}: {e}")
        logger.info(f"Cleaned up {len(files)} temporary files")
    except Exception as e:
        logger.error(f"Error during cleanup: {e}")


def init_worker(sum_features):
    """Initialize worker process with shared data"""
    global shared_features
    shared_features = sum_features

    
def perform_spatial_join(chunk, index):
    try:
        logger.info(f"                  started a new process to handle the chunk {index+1}")
        global shared_features
        result = gpd.sjoin(shared_features, chunk, how='right', predicate='intersects')
        # output_file = f"/tmp/chunk_result_{index+1}.parquet"
        # result.to_parquet(output_file)
        output_file = f"/tmp/chunk_result_{index+1}.gpkg" 
        result.to_file(output_file, layer="data", driver="GPKG")
        logger.info(f"                     processing the chunk {index+1} was done: {result.shape}")
        return output_file
    except Exception as e:
        logger.error(f"                    Error in chunk {index+1}: {e}")
        raise  # Re-raise the exception to handle it in the main process

def split_gdf(gdf, chunk_size):
    return [gdf.iloc[i:i + chunk_size] for i in range(0, len(gdf), chunk_size)]


def concurrent_join(in_polygons, in_sum_features):

    logger.info(f"               input data size: {in_polygons.shape}")
    in_polygons_chunks = split_gdf(in_polygons, 3000)
    logger.info(f"               split the data into {len(in_polygons_chunks)} chunks for concurrent processing")
    
    # Use ProcessPoolExecutor for parallel execution
    try:
        with ProcessPoolExecutor(max_workers=6, initializer=init_worker, initargs=(in_sum_features,)) as executor:
            futures = [executor.submit(perform_spatial_join, chunk, index) for index, chunk in enumerate(in_polygons_chunks)]
            results = []
            for future in futures:
                try:
                    # Set a timeout for each task
                    results.append(future.result(timeout=300))  # Timeout set to 300 seconds
                except TimeoutError:
                    logger.error("               A chunk processing timed out")
                except Exception as e:
                    logger.error(f"               An error occurred: {e}")
            result_files = [future.result() for future in futures]
            
            combined_result = None
            for f in result_files:
                logger.info(f"               combining: {f}")
                tmp = gpd.read_file(f, layer="data")
                if combined_result:
                    combined_result = gpd.GeoDataFrame(pd.concat([combined_result, tmp], ignore_index=True))
                del tmp
                    
            # combined_result = gpd.GeoDataFrame(pd.concat([gpd.read_file(f) for f in result_files], ignore_index=True))
    except Exception as e:
        logger.error(f"Critical error in processing: {e}")
    finally:
        # Clean up all processes if needed
        executor.shutdown(wait=True)
        cleanup_parquet_files()
        logger.info("Executor shut down cleanly")
