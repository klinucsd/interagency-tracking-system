
"""
# Description: Converts the Converts the California Department of Transportation's
#              fuels treatments dataset into the Task Force standardized schema.  
#              Dataset is enriched with vegetation, ownership, county, WUI, 
#              Task Force Region, and year.             
"""

import warnings
import logging
import time
import psutil
import os

import numpy as np
import pandas as pd
import geopandas as gpd
from datetime import datetime

from its_logging.logger_config import logger
from utils.its_utils import clip_to_california, get_wfr_tf_template
from utils.gdf_utils import repair_geometries, show_columns, verify_gdf_columns
from utils.add_common_columns import add_common_columns
from utils.enrich_polygons import enrich_polygons
from utils.keep_fields import keep_fields
from utils.assign_domains import assign_domains
from utils.save_gdf_to_gdb import save_gdf_to_gdb


logger = logging.getLogger('enrich.enrich_CalTrans')

# Suppress pyogrio INFO logs
logging.getLogger("pyogrio").setLevel(logging.WARNING)

# Suppress specific warnings
warnings.filterwarnings("ignore", category=RuntimeWarning)

# Suppress specific FutureWarning messages
warnings.simplefilter(action='ignore', category=FutureWarning)


CALTRANS_COLUMNS = [
    'UNIQUE_ID', 'SYS_CD', 'SYS_TRTMNT_ID', 'TRTMNT_NM', 'TRTMNT_TYPE_CD',
    'TRTMNT_SUBTYPE', 'TRTMNT_START_DT', 'TRTMNT_END_DT', 'TRTMNT_COMMENTS',
    'BLM_ACRES', 'GIS_ACRES', 'ADMIN_ST', 'Tmp_Text_ca', 'Tmp_Long_ca',
    'Tmp_Float_ca', 'Tmp_Date_ca', 'Comments_ca', 'CREATE_DATE', 'CREATE_BY',
    'MODIFY_DATE', 'MODIFY_BY', 'SHAPE_Length', 'SHAPE_Area', 'OBJECTID',
    'geometry'
]


def enrich_CalTrans(calTrans_gdb_path,
                    calTrans_layer_name, 
                    a_reference_gdb_path,
                    start_year,
                    end_year,
                    output_gdb_path,
                    output_layer_name):

    logger.info("Load the CalTRANS data into a GeoDataFrame")
    start = time.time()
    # calTrans = gpd.read_file(calTrans_gdb_path, driver="OpenFileGDB", layer=calTrans_layer_name)
    calTrans = gpd.read_file(calTrans_gdb_path, driver="OpenFileGDB", sql_dialect="OGRSQL", sql=f"SELECT *, OBJECTID FROM {calTrans_layer_name}")
    logger.info(f"   time for loading {calTrans_layer_name}: {time.time()-start}")
    
    # validate the input data
    verify_gdf_columns(calTrans, CALTRANS_COLUMNS, logger)
    
    calTrans = calTrans.to_crs(3310)
    show_columns(logger, calTrans, "calTrans")

    logger.info("Performing Standardization...")

    logger.info("   step 1/15 Clip Features to California...")
    calTrans_clip = clip_to_california(calTrans, a_reference_gdb_path)


    
if __name__ == "__main__":
    # Get the current process ID
    process = psutil.Process(os.getpid())
    
    calTrans_input_gdb_path = "b_Originals/CALTRANS.gdb"
    calTrans_input_layer_name = "CalTRANS_20230813"
    a_reference_gdb_path = "a_Reference.gdb"
    start_year, end_year = 2010, 2025
    output_gdb_path = f"/tmp/CalTRANS_{start_year}_{end_year}.gdb"
    output_layer_name = f"CalTRANS_enriched_{datetime.today().strftime('%Y%m%d')}"

    enrich_CALTRANS(calTrans_input_gdb_path,
                    calTrans_input_layer_name,
                    a_reference_gdb_path,
                    start_year,
                    end_year,
                    output_gdb_path,
                    output_layer_name)

    # Get memory usage in bytes, convert to MB
    memory_usage = process.memory_info().rss / 1024 / 1024
    logger.info(f"Memory usage: {memory_usage:.2f} MB")
    




    
