
"""
# Description: Converts the Timber Industry Spatial dataset into
#              the Task Force standardized schema.  Dataset is
#              enriched with vegetation, ownership, county, WUI, 
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


logger = logging.getLogger('enrich.Timber_Industry')

# Suppress pyogrio INFO logs
logging.getLogger("pyogrio").setLevel(logging.WARNING)

# Suppress specific warnings
warnings.filterwarnings("ignore", category=RuntimeWarning)

# Suppress specific FutureWarning messages
warnings.simplefilter(action='ignore', category=FutureWarning)

# Required columns for timber industry spatial data
TIMBER_INDUSTRY_SPATIAL_COLUMNS = [
    'Organization', 'Org_Public', 'ID', 'Name', 'Objective', 'Status', 
    'Year', 'BroadVegType', 'GISACRES', 'Shape_Length', 'Shape_Area', 
    'OBJECTID', 'geometry', 
]


def enrich_Timber_Industry(ti_gdb_path,
                           ti_layer_name, 
                           a_reference_gdb_path,
                           start_year,
                           end_year,
                           output_gdb_path,
                           output_layer_name):

    logger.info("Load the Timeber Industry Spatial Layer into a GeoDataFrame")
    
    start = time.time()
    # ti = gpd.read_file(ti_gdb_path, driver="OpenFileGDB", layer=ti_layer_name)
    ti = gpd.read_file(ti_gdb_path, driver="OpenFileGDB", sql_dialect="OGRSQL", sql=f"SELECT *, OBJECTID FROM {ti_layer_name}")
    logger.info(f"   time for loading {ti_layer_name}: {time.time()-start}")

    # validate the input data
    verify_gdf_columns(ti, TIMBER_INDUSTRY_SPATIAL_COLUMNS, logger)
    
    ti = ti.to_crs(3310)
    # ti = ti.loc[ti['Status'] == 'Exists']           # need to validate
    ti['Status'] = ti['Status'].replace({'Exists': 'Active'})
    show_columns(logger, ti, "ti")
    
    logger.info("Performing Standardization...")
    
    logger.info("   step 1/15 Clip Features to California...")
    start = time.time()
    ti_clip = clip_to_california(ti, a_reference_gdb_path)
    logger.info(f"      time for loading California and clipping: {time.time()-start}")
    show_columns(logger, ti_clip, "ti_clip")
    
    logger.info("   step 2/15 Repairing Geometry...")
    ti_clip = repair_geometries(ti_clip)

    logger.info("   step 3/15 Adding Common Columns...")
    standardized_ti = add_common_columns(ti_clip)
    show_columns(logger, standardized_ti, "standardized_ti")
    
    logger.info("   step 4/15 Transfering Values...")
    standardized_ti["PROJECTID_USER"] = 'TI-' + standardized_ti['OBJECTID'].astype(str) # need to validate
    
    standardized_ti["AGENCY"] = "TIMBER"                                                # need to validate
    standardized_ti["IMPLEMENTING_ORG"] = standardized_ti["Organization"]               # need to validate                            
    standardized_ti["ORG_ADMIN_p"] = "TIMBER"                                           # need to validate
    standardized_ti["ORG_ADMIN_t"] = "TIMBER"                                           # need to validate
    standardized_ti["ORG_ADMIN_a"] = "TIMBER"                                           # need to validate

    standardized_ti["PROJECT_CONTACT"] = None                               
    standardized_ti["PROJECT_EMAIL"] = None
    standardized_ti["ADMINISTERING_ORG"] = "TIMBER"                                     # need to validate
    standardized_ti["PROJECT_NAME"] = standardized_ti["Name"]                           # need to validate
    standardized_ti["PRIMARY_FUNDING_SOURCE"] = "PRIVATE"
    standardized_ti["PRIMARY_FUNDING_ORG"] = "TIMBER"
    standardized_ti["ACTIVITY_NAME"] = standardized_ti["Name"]                          # need to validate                                  
    standardized_ti["BVT_USERD"] = "NO"
    
    logger.info("   step 5/15 Calculating Start and End Date...")

    # Ensure Year is a proper numeric type and convert to integer
    standardized_ti['Year'] = pd.to_numeric(standardized_ti['Year'], errors='coerce')

    # Set ACTIVITY_START using to_datetime for more robust parsing
    standardized_ti['ACTIVITY_START'] = pd.to_datetime(
        standardized_ti['Year'].apply(lambda x: f"{x:.0f}-01-01" if pd.notnull(x) else None), 
        errors='coerce'
    )

    # Set ACTIVITY_END similarly
    standardized_ti['ACTIVITY_END'] = pd.to_datetime(
        standardized_ti['Year'].apply(lambda x: f"{x:.0f}-12-31" if pd.notnull(x) else None), 
        errors='coerce'
    )

    logger.info("   step 6/15 Calculating Status...")
    standardized_ti["ACTIVITY_STATUS"] = standardized_ti['Status'].str.upper()             # Need to validate
    standardized_ti["PROJECT_STATUS"] = standardized_ti['Status'].str.upper()              # Need to validate

    logger.info("   step 7/15 Activity Quantity...")
    standardized_ti["ACTIVITY_QUANTITY"] = standardized_ti["GISACRES"]
    standardized_ti["ACTIVITY_QUANTITY"] = standardized_ti["ACTIVITY_QUANTITY"].astype(float)
    standardized_ti["ACTIVITY_UOM"] = "AC"
    
    logger.info("   step 8/15 Enter Column Values...")
    standardized_ti["ADMIN_ORG_NAME"] = "TIMBER"                                            # Need to validate
    standardized_ti["IMPLEM_ORG_NAME"] = standardized_ti["Organization"]                    # Need to validate
    standardized_ti['PRIMARY_FUND_SRC_NAME'] = standardized_ti['PRIMARY_FUNDING_SOURCE']
    standardized_ti['PRIMARY_FUND_ORG_NAME'] = standardized_ti['PRIMARY_FUNDING_ORG']
    standardized_ti["Source"] = "Industrial Timber"                                         # Need to validate

    logger.info("   step 9/15 Adding Original Activity Description to Crosswalk Column...")  

    def ifelse(activity):
        if activity == "Fuel Break":
            return "Thinning (Manual)"
        elif activity == "Broadcast Burn":
            return None
        elif activity == "Right of Way Clearance":
            return None
        elif activity == "Fuel Reduction":
            return None
        else:
            return activity
    # standardized_ti['Crosswalk'] = standardized_ti['ACTIVITY_DESCRIPTION'].apply(ifelse)

    standardized_ti['ACTIVITY_DESCRIPTION'] = standardized_ti['Objective']
    standardized_ti['Crosswalk'] = standardized_ti['ACTIVITY_DESCRIPTION']
    
    logger.info("   step 10/15 Select by Years...")
    selected_gdf = standardized_ti[(standardized_ti["Year"] >= start_year) & (standardized_ti["Year"] <= end_year)]
    show_columns(logger, selected_gdf, "selected_gdf")

    logger.info("   step 10/15 Create New GeoDataframe Using the Template...")
    new_ti = gpd.GeoDataFrame(columns=get_wfr_tf_template(a_reference_gdb_path).columns, crs="EPSG:3310")  
    show_columns(logger, new_ti, "new_ti")
    
    logger.info("   step 10/15 Append to Template...")
    new_ti = pd.concat([new_ti, selected_gdf], ignore_index=True)
    show_columns(logger, new_ti, "concat")
    
    logger.info("   step 10/15 Calculate Treatment Geometry...")
    new_ti["TRMT_GEOM"] = "POLYGON"

    logger.info("   step 11/15 Remove Unnecessary Columns...")
    new_ti = keep_fields(new_ti)
    show_columns(logger, new_ti, "keep_fields")
    
    logger.info("   step 12/15 Enriching Polygons...")
    enriched_ti = enrich_polygons(new_ti, a_reference_gdb_path, start_year, end_year)  
    
    logger.info("   step 13/15 Calculate Treatment ID...")
    enriched_ti["TRMTID_USER"] = enriched_ti["PROJECTID_USER"]

    show_columns(logger, enriched_ti, "enriched_ti")

    logger.info("   step 14/15 Assign Domains...")
    enriched_ti = assign_domains(enriched_ti)
    
    logger.info("   step 15/15 Save Result...")
    # enriched_ti.to_file('c_Enriched/Timber_Industry_Spatial_enriched.geojson', driver='GeoJSON')
    save_gdf_to_gdb(enriched_ti,
                    output_gdb_path,
                    output_layer_name,
                    group_name="c_Enriched")
    
    
if __name__ == "__main__":
    # Get the current process ID
    process = psutil.Process(os.getpid())

    ti_input_gdb_path = "b_Originals/FFSC_MOU_2023_20240627_RebeccaFerkovichViaEmail.gdb"
    ti_input_layer_name = "FFSC_MOU_IndustryOnly_Pol"
    a_reference_gdb_path = "a_Reference.gdb"
    start_year, end_year = 2021, 2023
    output_gdb_path = f"/tmp/Timber_Industry_Spatial_{start_year}_{end_year}.gdb"
    output_layer_name = f"Timber_Industry_Spatial_{datetime.today().strftime('%Y%m%d')}"
    
    enrich_Timber_Industry(ti_input_gdb_path,
                           ti_input_layer_name,
                           "a_Reference.gdb",
                           start_year,
                           end_year,
                           output_gdb_path,
                           output_layer_name)

    # Get memory usage in bytes, convert to MB
    memory_usage = process.memory_info().rss / 1024 / 1024
    logger.info(f"Memory usage: {memory_usage:.2f} MB")
