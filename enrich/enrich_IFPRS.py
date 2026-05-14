"""
# Description: Converts the U.S. Department of Interior, Integrated Fuels
#              Prioritization and Reporting System (IFPRS) actual treatments
#              dataset into the Task Force standardized schema. Dataset
#              is enriched with vegetation, ownership, county, WUI,
#              Task Force Region, and year. Only processes records for
#              California with Completion date after 10/1/2023.
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

from its_logging.logger_config import logger
from utils.its_utils import clip_to_california, get_wfr_tf_template
from utils.gdf_utils import repair_geometries, show_columns, verify_gdf_columns
from utils.add_common_columns import add_common_columns
from utils.enrich_polygons import enrich_polygons
from utils.keep_fields import keep_fields
from utils.assign_domains import assign_domains
from utils.save_gdf_to_gdb import save_gdf_to_gdb

logger = logging.getLogger('enrich.enrich_IFPRS')
logger.setLevel(logging.INFO)

# Suppress pyogrio INFO logs
logging.getLogger("pyogrio").setLevel(logging.WARNING)

# Suppress specific warnings
warnings.filterwarnings("ignore", category=RuntimeWarning)

# Suppress specific FutureWarning messages
warnings.simplefilter(action='ignore', category=FutureWarning)

IFPRS_COLUMNS = ["OBJECTID", "ProjectID", "Agency", "ProjectName", "Name", "InitiationDate", "CompletionDate",
                 "Status", "MeasureAmount", "UnitOfMeasure", "Shape_Area", "FundingSource", "FundingAgency",
                 "Type", "SubType"]

def enrich_IFPRS(ifprs_gdb_path,
                 ifprs_layer_name,
                 a_reference_gdb_path,
                 start_year,
                 end_year,
                 output_gdb_path,
                 output_layer_name):

    logger.info("Load the IFPRS data into a GeoDataFrame")
    start = time.time()
    ifprs = gpd.read_file(ifprs_gdb_path, driver="OpenFileGDB", sql_dialect="OGRSQL", sql=f"SELECT *, OBJECTID FROM {ifprs_layer_name}")
    logger.info(f"   time for loading {ifprs_layer_name}: {time.time()-start}")
    logger.debug(f"      ifprs shape: {ifprs.shape}")
    
    # Validate the input data
    verify_gdf_columns(ifprs, IFPRS_COLUMNS, logger)

    # Filter to California and post-10/1/2023 records
    logger.info("   Filtering to California and Completion after 2023-10-01...")
    ifprs = ifprs[ifprs['State'] == 'California']
    logger.debug(f"      ifprs shape after filtering State by California: {ifprs.shape}")

    ifprs['CompletionDate'] = pd.to_datetime(ifprs['CompletionDate'], errors='coerce', utc=True)
    ifprs = ifprs[ifprs['CompletionDate'] >= pd.Timestamp('2023-10-01', tz='UTC')]
    ifprs = ifprs.to_crs(3310)
    logger.debug(f"      ifprs shape after filtering by 2023-10-01: {ifprs.shape}")
     
    logger.info("Performing Standardization...")

    logger.info("   step 1/15 Clip Features to California...")
    ifprs_clip = clip_to_california(ifprs, a_reference_gdb_path)
    logger.info(f"      ifprs shape after clip_to_california: {ifprs_clip.shape}")
    
    logger.info("   step 2/15 Repairing Geometry...")
    ifprs_clip = repair_geometries(ifprs_clip)
    logger.info(f"      ifprs shape after repairing geometry: {ifprs_clip.shape}")
    
    logger.info("   step 3/15 Adding Common Columns...")
    standardized_ifprs = add_common_columns(ifprs_clip)
    show_columns(logger, standardized_ifprs, "standardized_ifprs")
    logger.info(f"      ifprs shape after add_common_columns: {standardized_ifprs.shape}")
    
    logger.info("   step 4/15 Transferring Values...")
    standardized_ifprs["PROJECTID_USER"] = standardized_ifprs["ProjectID"].astype(str)
    standardized_ifprs["AGENCY"] = "DOI"
    standardized_ifprs["ORG_ADMIN_p"] = "IFPRS"
    standardized_ifprs["ORG_ADMIN_t"] = "IFPRS"
    standardized_ifprs["ORG_ADMIN_a"] = "IFPRS"
    standardized_ifprs["PROJECT_CONTACT"] = None
    standardized_ifprs["PROJECT_EMAIL"] = None
    standardized_ifprs["ADMINISTERING_ORG"] = standardized_ifprs["Agency"]
    standardized_ifprs["PROJECT_NAME"] = standardized_ifprs["ProjectName"]
    standardized_ifprs["ACTIVITY_NAME"] = standardized_ifprs["Name"]
    standardized_ifprs["BVT_USERD"] = "NO"
    standardized_ifprs["ACTIVITYID_USER"] = standardized_ifprs["OBJECTID"].apply(lambda x: "IFPRS" + str(x))


    logger.info("   step 5/15 Calculating Start and End Date...")
    def safe_date_convert(x):
        if pd.isna(x):
            return None
        try:
            return pd.to_datetime(x, errors='coerce', utc=True)
        except Exception as e:
            logger.error(f"      Problem converting date: {x}, Error: {e}")
            return None

    standardized_ifprs["ACTIVITY_START"] = standardized_ifprs["InitiationDate"].apply(safe_date_convert)
    standardized_ifprs["ACTIVITY_END"] = standardized_ifprs["CompletionDate"].apply(safe_date_convert)

    logger.debug("-"*70)
    logger.debug(standardized_ifprs[["ACTIVITY_START", "ACTIVITY_END"]])

    logger.info("   step 6/15 Calculating Status...")
    def map_status(status):
        status_map = {
            'Cancelled': 'Cancelled',
            'Completed': 'Complete',
            'Not Started': 'Planned',
            'Started': 'Active',
            'Draft': 'Outyear',
            'Ready for Approval': 'Proposed',
            'Approved': 'Proposed',
            'Approved (Local)': 'Proposed',
            'Approved (Regional)': 'Proposed',
            'Approved (Agency)': 'Proposed',
            'Approved (Department)': 'Proposed',
            'UnApproval Requested': 'Proposed'

        }
        return status_map.get(status, None)

    standardized_ifprs["ACTIVITY_STATUS"] = standardized_ifprs["Status"].apply(map_status)
    
    logger.info("   step 7/15 Activity Quantity...")
    def calculate_quantity(total_acres, gis_acres):
        if pd.isna(total_acres) or total_acres == 0:
            return gis_acres
        return total_acres

    standardized_ifprs["TotalAcres"] = standardized_ifprs["MeasureAmount"]
    standardized_ifprs["ACTIVITY_QUANTITY"] = standardized_ifprs.apply(
        lambda row: calculate_quantity(row["TotalAcres"], row["Shape_Area"] / 4046.86), axis=1
    )
    standardized_ifprs["ACTIVITY_QUANTITY"] = standardized_ifprs["ACTIVITY_QUANTITY"].astype(float)

    def map_uom(uom):
        uom_map = {'Acres': 'Acres',
                   'Feet / Miles': 'Miles',
                   'Each (Number)': 'Each'}
        return uom_map.get(uom, None)

    def fill_uom(uom, area_unit):
        if pd.isna(uom):
            return area_unit
        return uom
    standardized_ifprs["ACTIVITY_UOM"] = standardized_ifprs["UnitOfMeasure"].apply(map_uom)
    standardized_ifprs["ACTIVITY_UOM"] = standardized_ifprs.apply(
        lambda row: fill_uom(row['ACTIVITY_UOM'], row['AreaUnit']), axis=1)

    logger.debug("-"*70)
    logger.debug(standardized_ifprs[["TotalAcres", "Shape_Area", "ACTIVITY_QUANTITY"]])

    logger.info("   step 8/15 Enter Column Values...")
    standardized_ifprs["PRIMARY_FUNDING_SOURCE"] = "FEDERAL"
    
    standardized_ifprs["IMPLEMENTING_ORG"] = standardized_ifprs["Agency"]
    standardized_ifprs["ADMIN_ORG_NAME"] = standardized_ifprs["Agency"]
    standardized_ifprs["IMPLEM_ORG_NAME"] = standardized_ifprs["Agency"]
    standardized_ifprs["PRIMARY_FUND_SRC_NAME"] = standardized_ifprs["FundingSource"]
    standardized_ifprs["PRIMARY_FUNDING_ORG"] = standardized_ifprs["FundingAgency"]
    standardized_ifprs["PRIMARY_FUND_ORG_NAME"] = standardized_ifprs["FundingAgency"]
    standardized_ifprs["Source"] = "IFPRS"

    standardized_ifprs["Year"] = standardized_ifprs["ACTIVITY_END"].apply(
        lambda x: x.year if pd.notnull(x) else None
    )

    standardized_ifprs["Federal_FY"] = standardized_ifprs["CompletionFiscalYear"]

    logger.info("   step 9/15 Adding Activity Description to Crosswalk Column...")
    def crosswalk_activity(type_val, subtype_val):
        
        type_val = str(type_val).lower() if pd.notnull(type_val) else ""
        subtype_val = str(subtype_val).lower() if pd.notnull(subtype_val) else ""

        # Excel crosswalk mappings
        if type_val == "wildfire":
            return "NOT_DEFINED"
        elif type_val in ["broadcast", "underburn"]:
            return "BROADCAST_BURN"
        elif type_val == "pile burn":
            return "PILE_BURN"
        elif type_val == "thinning":
            return "THIN_MECH"
        elif type_val == "clearcut, chaining":
            return "CHAIN_CRUSH"
        elif type_val == "pile":
            return "PILING"
        elif type_val == "lop and scatter":
            return "LOP_AND_SCAT"
        elif type_val == "fuel compaction":
            if subtype_val in ["grinding", "mastication"]:
                return "MASTICATION"
            elif subtype_val == "crushing":
                return "CHAIN_CRUSH"
            elif subtype_val == "chipping":
                return "CHIPPING"
            elif subtype_val == "mowing":
                return "MOWING"
            else:
                # subtype '' and None are also MASTICATION
                return "MASTICATION"

        elif type_val == "grinding, chipping, crushing":
            return "MASTICATION"
        elif type_val == "biomass removal":
            return "BIOMASS_REMOVAL"
        elif type_val == "herbicide":
            return "HERBICIDE_APP"
        elif type_val in ["insecticide", "biocontrol"]:
            return "PEST_CNTRL"
        elif type_val == "planting":
            return "TREE_PLNTING"
        elif type_val == "seeding":
            return "TREE_SEEDING"
        elif type_val == "herbivory":
            return "PRESCRB_HERBIVORY"

        return "NOT_DEFINED"

    standardized_ifprs["Crosswalk"] = standardized_ifprs.apply(
        lambda row: crosswalk_activity(row["Type"], row["SubType"]), axis=1
    )
    logger.debug(standardized_ifprs["Crosswalk"])

    logger.info("   step 10/15 Select by Years...")
    selected_gdf = standardized_ifprs[(standardized_ifprs["Year"] >= start_year) &
                                    (standardized_ifprs["Year"] <= end_year)]

    logger.info("   step 11/15 Create New GeoDataFrame Using the Template...")
    new_ifprs = gpd.GeoDataFrame(columns=get_wfr_tf_template(a_reference_gdb_path).columns, crs="EPSG:3310")

    logger.info("   step 12/15 Append to Template...")
    new_ifprs = pd.concat([new_ifprs, selected_gdf], ignore_index=True)

    logger.info("   step 13/15 Calculate Treatment Geometry...")
    new_ifprs["TRMT_GEOM"] = "POLYGON"

    logger.info("   step 14/15 Remove Unnecessary Columns...")
    new_ifprs = keep_fields(new_ifprs)
    show_columns(logger, new_ifprs, "new_ifprs")

    logger.info("   step 15/15 Enriching Polygons...")
    enriched_ifprs = enrich_polygons(new_ifprs, a_reference_gdb_path, start_year, end_year)

    logger.info("   step 16/15 Calculate Treatment ID...")
    enriched_ifprs["TRMTID_USER"] = (
        enriched_ifprs["PROJECTID_USER"] + "-" +
        enriched_ifprs["COUNTY"].str[:3] + "-" +
        enriched_ifprs["PRIMARY_OWNERSHIP_GROUP"].str[:4] + "-" +
        enriched_ifprs["IN_WUI"].str[:3] + "-" +
        enriched_ifprs["PRIMARY_OBJECTIVE"].str[:8]
    )

    show_columns(logger, enriched_ifprs, "enriched_ifprs")
    
    logger.info("   step 17/15 Assign Domains...")
    enriched_ifprs = assign_domains(enriched_ifprs)

    # agency none temp fix
    enriched_ifprs["AGENCY"] = "DOI"

    logger.info("   step 18/15 Save Result...")
    save_gdf_to_gdb(enriched_ifprs,
                    output_gdb_path,
                    output_layer_name,
                    group_name="c_Enriched")

if __name__ == "__main__":
    # Get the current process ID
    process = psutil.Process(os.getpid())

    # load config file path yaml
    with open("..\config.yaml", 'r') as stream:
        config_inputs = yaml.safe_load(stream)

    ifprs_input_gdb_path = config_inputs['sources']['ifprs']['input']['gdb_path']
    ifprs_input_layer_name = config_inputs['sources']['ifprs']['input']['layer_name']
    a_reference_gdb_path = config_inputs['global']['reference_gdb']
    start_year, end_year = config_inputs['global']['start_year'], config_inputs['global']['end_year']
    output_format_dict = {'start_year': start_year,
                          'end_year': end_year,
                          'date': datetime.today().strftime('%Y%m%d')}
    output_gdb_path = config_inputs['sources']['ifprs']['output']['gdb_path'].format(**output_format_dict)
    output_layer_name = config_inputs['sources']['ifprs']['output']['layer_name'].format(**output_format_dict)


    enrich_IFPRS(ifprs_input_gdb_path,
                 ifprs_input_layer_name,
                 a_reference_gdb_path,
                 start_year,
                 end_year,
                 output_gdb_path,
                 output_layer_name)

    # Get memory usage in bytes, convert to MB
    memory_usage = process.memory_info().rss / 1024 / 1024
    logger.info(f"Memory usage: {memory_usage:.2f} MB")
