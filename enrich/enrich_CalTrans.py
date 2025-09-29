
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
import yaml

import numpy as np
import pandas as pd
import geopandas as gpd
from datetime import datetime
from shapely.geometry.base import BaseGeometry
from typing import List, Tuple, Union, Optional

from its_logging.logger_config import logger
from utils.its_utils import clip_to_california, get_wfr_tf_template
from utils.gdf_utils import repair_geometries, show_columns, verify_gdf_columns, get_rows_with_empty_geometry
from utils.add_common_columns import add_common_columns
from utils.enrich_lines import enrich_lines
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

warnings.filterwarnings("ignore", message="Measured \\(M\\) geometry types are not supported.*")


CALTRANS_TREE_ACTIVITY_COLUMNS = [
    'Resp_District', 'Work_Order_Number', 'Begin_County', 'End_County', 'Route',
    'Route_Suffix', 'From_PM_Prefix', 'From_PM', 'From_PM_Suffix', 'From_PM_C',
    'To_PM_Prefix', 'To_PM', 'To_PM_Suffix', 'To_PM_C', 'Production_Quantity',
    'UOM', 'Maintenance_Type', 'Comments', 'Activity', 'Activity_Description',
    'IMMS_Unit_ID', 'Fiscal_Year', 'From_Miles', 'To_Miles', 'Highway_ID',
    'Shape_Length', 'BeginWorkDate', 'EndWorkDate', 'ACTIVITY_STATUS',
    'BROAD_VEGETATION_TYPE', 'RESIDUE_QUANTITY', 'RESIDUE_FATE',
    'ACTIVITY_PERCENT_COMPLETE', 'ActivityID', 'ACTIVITY_NAME',
    'PRIMARY_FUNDING_SOURCE_NAME', 'PRIMARY_FUNDING_ORG_NAME',
    'SECONDARY_FUNDING_SOURCE_NAME', 'SECONDARY_FUNDING_ORG__NAME',
    'TERTIARY_FUNDING_SOURCE_NAME', 'TERTIARY_FUNDING_ORG_NAME',
    'ADMINISTERING_ORG_NAME', 'IMPLEMENTING_ORG_NAME',
    'Route_Num', 'Calendar_Year', 'District', 'Route_C'
]

CALTRANS_ROAD_ACTIVITY_COLUMNS = [
    'Resp_District', 'Work_Order_Number', 'Begin_County', 'End_County', 'Route_Num',
    'Route', 'Route_Suffix', 'From_PM_Prefix', 'From_PM', 'From_PM_Suffix', 'From_PM_C',
    'To_PM_Prefix', 'To_PM', 'To_PM_Suffix', 'To_PM_C', 'Production_Quantity', 'UOM',
    'Maintenance_Type', 'Comments', 'Activity', 'Activity_Description', 'Highway_ID',
    'IMMS_Unit_ID', 'Fiscal_Year', 'From_Miles', 'To_Miles', 'WorkBeginDate',
    'WorkEndDate', 'ACTIVITY_NAME', 'PRIMARY_FUNDING_SOURCE_NAME',
    'PRIMARY_FUNDING_ORG_NAME', 'SECONDARY_FUNDING_SOURCE_NAME',
    'SECONDARY_FUNDING_ORG__NAME', 'TERTIARY_FUNDING_SOURCE_NAME',
    'TERTIARY_FUNDING_ORG_NAME', 'ADMINISTERING_ORG_NAME', 'IMPLEMENTING_ORG_NAME',
    'ACTIVITY_STATUS', 'BROAD_VEGETATION_TYPE', 'RESIDUE_QUANTITY', 'RESIDUE_FATE',
    'ACTIVITY_PERCENT_COMPLETE', 'ActivityID', 'Calendar_Year',
    'District', 'Route_C'
]

CALTRANS_TREE_TREATMENT_COLUMNS = [
    'District_txt', 'District_num', 'County', 'Route_num', 'Route_txt',
    'Route_suffix', 'Highway_ID', 'FREQUENCY', 'SUM_Production_Quantity',
    'Ownership_Group', 'Primary_Objective', 'Secondary_Objective',
    'Tertiary_Objective', 'Estimated_Retreatment_Date', 'Treatment_Status',
    'Calendar_Year', 'Shape_Length', 'geometry'
]

CALTRANS_ROAD_TREATMENT_COLUMNS = [
    'District_txt', 'District_num', 'County', 'Route_num', 'Route_txt',
    'Route_suffix', 'Highway_ID', 'FREQUENCY', 'SUM_Production_Quantity',
    'Ownership_Group', 'Primary_Objective', 'Secondary_Objective',
    'Tertiary_Objective', 'Estimated_Retreatment_Date', 'Treatment_Status',
    'Calendar_Year', 'Shape_Length', 'geometry'
]


def calc_treatment_area(uom: str, quantity: float) -> Union[float, None]:
    """Calculate treatment area based on unit of measure"""
    if uom in ["ACRE", "AC"]:
        return quantity
    return None

def convert_uom(uom: str) -> str:
    """Convert unit of measure to standardized format"""
    if uom == 'ACRE':
        return 'AC'
    return uom

def caltrans(
        caltrans_table,
        caltrans_lines,
        a_reference_gdb_path,
        start_year,
        end_year,
        output_gdb_path,
        output_layer_name
) -> None:

# Merge data - modified to handle geometry correctly

    logger.info("Performing Standardization")
    logger.info("   step 1/10 merge treatments and activities")
    merged_data = caltrans_lines.merge(
        caltrans_table, 
        how='inner', 
        on=['Highway_ID', 'Calendar_Year']
    )
    logger.info(f"      merged_data has {len(merged_data)} records")
    
    # Repair geometry
    logger.info("   step 2/10 repair geometries")
    invalid_geoms = (~merged_data.geometry.is_valid).sum()
    if invalid_geoms > 0:
        # merged_data = repair_geometries(merged_data)
        logger.error("Found invalid geometries")
        exit()
        
    # Add common columnss
    logger.info("   step 3/10 add standard columns")
    merged_data = add_common_columns(merged_data)
    
    # Calculate fields
    logger.info("   step 4/10 calculate column values")
    merged_data['PROJECTID_USER'] = merged_data['Highway_ID']
    merged_data['AGENCY'] = 'CALSTA'
    merged_data['ORG_ADMIN_p'] = 'CALTRANS'
    merged_data['ORG_ADMIN_t'] = 'CALTRANS'
    merged_data['ORG_ADMIN_a'] = 'CALTRANS'
    merged_data['PROJECT_CONTACT'] = 'Division of Maintenance'
    merged_data['PROJECT_EMAIL'] = 'andrew.lozano@dot.ca.gov'
    merged_data['ADMINISTERING_ORG'] = 'CALTRANS'
    merged_data['ADMIN_ORG_NAME'] = 'CALTRANS'
    merged_data['PRIMARY_FUNDING_SOURCE'] = 'GENERAL_FUND'
    merged_data['PRIMARY_FUND_SRC_NAME'] = 'GENERAL_FUND'
    merged_data['PRIMARY_FUNDING_ORG'] = 'CALTRANS'
    merged_data['PRIMARY_FUND_ORG_NAME'] = 'CALTRANS'
    merged_data['TRMTID_USER'] = merged_data.apply(
        lambda x: f"{x['Highway_ID']}-{x['From_PM_C']}-{x['To_PM_C']}", axis=1
    )
    merged_data['TREATMENT_AREA'] = merged_data.apply(
        lambda x: calc_treatment_area(x['UOM'], x['Production_Quantity']), axis=1
    )
    merged_data['ACTIVID_USER'] = merged_data.apply(
        lambda x: f"CALTRANS-{x['Work_Order_Number']}-{x.name}", axis=1
    )
    merged_data['IMPLEMENTING_ORG'] = merged_data['District']
    merged_data['IMPLEM_ORG_NAME'] = merged_data['District']
    merged_data['ACTIVITY_UOM'] = merged_data['UOM'].apply(convert_uom)
    merged_data['ACTIVITY_QUANTITY'] = merged_data['Production_Quantity']
    merged_data['ACTIVITY_STATUS'] = 'COMPLETE'
    merged_data['ACTIVITY_START'] = merged_data['WorkBeginDate']
    merged_data['ACTIVITY_END'] = merged_data['WorkEndDate']
    merged_data['Source'] = 'CALTRANS'
    merged_data['Crosswalk'] = merged_data['Activity_Description']
    merged_data['TRMT_GEOM'] = 'LINE'
    
    # Keep required fields
    logger.info("   step 5/10 keep standard columns only")
    merged_data = keep_fields(merged_data)

    # Enrich data
    logger.info("   step 6/10 calculate broad veg table, region and etc")
    enriched_data = enrich_lines(merged_data, a_reference_gdb_path, start_year, end_year)    

    logger.info(f"      enriched data has {len(enriched_data)} records")
    
    # Final calculations
    logger.info("   step 7/10 calculate PRIMARY_OWNERSHIP_GROUP and TRMTID_USER")
    enriched_data['PRIMARY_OWNERSHIP_GROUP'] = 'STATE'
    enriched_data['TRMTID_USER'] = enriched_data.apply(
        lambda x: f"{x['PROJECTID_USER']}-{str(x['COUNTY'])[:8]}-{str(x['REGION'])[:3]}-{str(x['IN_WUI'])[:3]}",
        axis=1
    )

    logger.info("   step 8/10 Remove Unnecessary Columns...")
    enriched_data = keep_fields(enriched_data)
    
    logger.info("   step 9/10 Assign Domains...")
    enriched_data = assign_domains(enriched_data)
    
    logger.info("   step 10/10 Save Result...")
    save_gdf_to_gdb(enriched_data,
                    output_gdb_path,
                    output_layer_name,
                    group_name="c_Enriched")


def enrich_Caltrans(caltrans_input_gdb_path,
                    tree_activity_layer_name,
                    tree_treatment_layer_name,
                    road_activity_layer_name,
                    road_treatment_layer_name,
                    a_reference_gdb_path,
                    start_year,
                    end_year,
                    output_gdb_path,
                    output_layer_name):

    """
    logger.info("Load Caltrans tree activity layer into a DataFrame")
    tree_activities = gpd.read_file(caltrans_input_gdb_path, driver="OpenFileGDB", sql_dialect="OGRSQL", sql=f"SELECT *  FROM {tree_activity_layer_name}")
    verify_gdf_columns(tree_activities, CALTRANS_TREE_ACTIVITY_COLUMNS, logger)
    show_columns(logger, tree_activities, "tree_activities")

    logger.info("Load Caltrans tree treatment layer into a GeoDataFrame")
    tree_treatments = gpd.read_file(caltrans_input_gdb_path, driver="OpenFileGDB", sql_dialect="OGRSQL", sql=f"SELECT * FROM {tree_treatment_layer_name}")
    verify_gdf_columns(tree_treatments, CALTRANS_TREE_TREATMENT_COLUMNS, logger)
    tree_treatments = tree_treatments.to_crs(3310)
    show_columns(logger, tree_treatments, "tree_treatments")
    """

    # remap col name to ITS col names
    activity_dict = {'Resp__District': 'Resp_District',
                    'Route_Txt': 'Route_C',
                    'Fiscal_Yr': 'Fiscal_Year',
                    'Work_Begin_Date': 'WorkBeginDate',
                    'Work_End_Date': 'WorkEndDate',
                    'Activity_Name': 'ACTIVITY_NAME',
                    'Primary_Funding_Source_Name': 'PRIMARY_FUNDING_SOURCE_NAME', 
                    'Primary_Funding_Org_Name': 'PRIMARY_FUNDING_ORG_NAME',
                    'Secondary_Funding_Source_Name': 'SECONDARY_FUNDING_SOURCE_NAME',
                    'Secondary_Funding_Source_Org': 'SECONDARY_FUNDING_ORG__NAME',
                    'Tertiary_Funding_Source_Name': 'TERTIARY_FUNDING_SOURCE_NAME', 
                    'Tertiary_Funding_Org_Name': 'TERTIARY_FUNDING_ORG_NAME',
                    'Administering_Org_Name': 'ADMINISTERING_ORG_NAME', 
                    'Implementing_Org_Name': 'IMPLEMENTING_ORG_NAME', 
                    'Activity_Status': 'ACTIVITY_STATUS',
                    'Broad_Vegetation_Type': 'BROAD_VEGETATION_TYPE', 
                    'Residue_Quantity': 'RESIDUE_QUANTITY', 
                    'Residue_Fate': 'RESIDUE_FATE',
                    'Activity_Percent_Complete': 'ACTIVITY_PERCENT_COMPLETE',
                    'District_Num': 'District'}
        
    treatment_dict = {'Route': 'Route_num',
                      'RouteS': 'Route_suffix',
                      'District': 'District_num'}
    

    logger.info("Load Caltrans road activity layer into a DataFrame")
    road_activities = gpd.read_file(caltrans_input_gdb_path, driver="OpenFileGDB", sql_dialect="OGRSQL", sql=f"SELECT *  FROM {road_activity_layer_name}")
    road_activities = road_activities.rename(activity_dict, axis=1)

    # create txt route field if not exist
    if 'Route' not in road_activities.columns:
        max_len = len(str(road_activities.Route_Num.max()))
        road_activities['Route'] = road_activities['Route_Num'].apply(lambda x: '0'*(max_len-len(str(x))) + str(x))

    

    verify_gdf_columns(road_activities, CALTRANS_ROAD_ACTIVITY_COLUMNS, logger)
    show_columns(logger, road_activities, "road_activities")

    logger.info("Load Caltrans road treatment layer into a GeoDataFrame")
    road_treatments = gpd.read_file(caltrans_input_gdb_path, driver="OpenFileGDB", sql_dialect="OGRSQL", sql=f"SELECT * FROM {road_treatment_layer_name}")
    road_treatments = road_treatments.rename(treatment_dict, axis=1)

    verify_gdf_columns(road_treatments, CALTRANS_ROAD_TREATMENT_COLUMNS, logger)
    road_treatments = road_treatments.to_crs(3310)
    show_columns(logger, road_treatments, "road_treatments")
    
    caltrans(
        road_activities,
        road_treatments,
        a_reference_gdb_path,
        start_year,
        end_year,
        output_gdb_path,
        output_layer_name)
    

    
if __name__ == "__main__":
    # Get the current process ID
    process = psutil.Process(os.getpid())

    # load config file path yaml
    with open("..\config.yaml", 'r') as stream:
        config_inputs = yaml.safe_load(stream)
    
    caltrans_input_gdb_path = config_inputs['caltrans']['input']['gdb_path']
    # tree layers are not used for this project for now
    tree_activity_layer_name = None
    tree_treatment_layer_name = None
    road_activity_layer_name = config_inputs['caltrans']['input']['road_activity_layer_name_path']
    road_treatment_layer_name = config_inputs['caltrans']['input']['road_treatment_layer_name']
    a_reference_gdb_path = config_inputs['global']['reference_gdb']
    start_year, end_year = config_inputs['global']['start_year'], config_inputs['global']['end_year']
    output_format_dict = {'start_year': start_year,
                          'end_year': end_year,
                          'date': datetime.today().strftime('%Y%m%d')}
    output_gdb_path = config_inputs['caltrans']['output']['gdb_path'].format(**output_format_dict)
    output_layer_name = config_inputs['caltrans']['output']['layer_name'].format(**output_format_dict)

    enrich_Caltrans(caltrans_input_gdb_path,
                    tree_activity_layer_name,
                    tree_treatment_layer_name,
                    road_activity_layer_name,
                    road_treatment_layer_name,
                    a_reference_gdb_path,
                    start_year,
                    end_year,
                    output_gdb_path,
                    output_layer_name)

    # Get memory usage in bytes, convert to MB
    memory_usage = process.memory_info().rss / 1024 / 1024
    logger.info(f"Memory usage: {memory_usage:.2f} MB")
    




    
