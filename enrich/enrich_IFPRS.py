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
logger.setLevel(logging.DEBUG)

# Suppress pyogrio INFO logs
logging.getLogger("pyogrio").setLevel(logging.WARNING)

# Suppress specific warnings
warnings.filterwarnings("ignore", category=RuntimeWarning)

# Suppress specific FutureWarning messages
warnings.simplefilter(action='ignore', category=FutureWarning)

IFPRS_COLUMNS = [
    'ActualTrea', 'IsPoint', 'CreatedBy', 'Name', 'Unit', 'Region',
    'Agency', 'Department', 'IsDepartme', 'Latitude', 'Longitude', 'Calculated',
    'IsWUI', 'Initiation', 'Initiati_1', 'Initiati_2', 'Completion', 'Completi_1',
    'Completi_2', 'Notes', 'LastModifi', 'CreatedOnD', 'LastModi_1', 'Status',
    'StatusReas', 'IsArchived', 'Class', 'Category', 'Type', 'SubType', 'Durability',
    'Priority', 'FundingSou', 'Congressio', 'County', 'State', 'EstimatedP',
    'EstimatedA', 'EstimatedG', 'EstimatedC', 'EstimatedO', 'EstimatedT', 'LocalAppro',
    'RegionalAp', 'AgencyAppr', 'Departme_1', 'FundedDate', 'EstimatedS', 'Feasibilit',
    'IsApproved', 'IsFunded', 'TribeName', 'TotalAcres', 'FundingUni', 'FundingReg',
    'FundingAge', 'FundingDep', 'FundingTri', 'WBSID', 'CostCenter', 'Functional',
    'CostCode', 'CancelledD', 'HasGroup', 'GroupCount', 'UnitID', 'VegDepartu',
    'VegDepar_1', 'IsVegetati', 'IsRTRL', 'FundingSub', 'FundingU_1', 'IsBIL',
    'BILFunding', 'TreatmentD', 'Contribute', 'Contribu_1', 'Contribu_2', 'Contribu_3',
    'Contribu_4', 'Contribu_5', 'Contribu_6', 'Contribu_7', 'Contribu_8', 'Contribu_9',
    'Contrib_10', 'FundingS_1', 'FundingS_2', 'FundingS_3', 'Obligation', 'CarryoverF',
    'IsCarryove', 'IsCanceled', 'GlobalID', 'EntityID', 'EntityType', 'EntityCate',
    'SubType', 'FundingU_2', 'OriginalIn', 'Original_1', 'Original_2', 'IsSagebrus',
    'UnApproval', 'UnApprov_1', 'IsFundingA', 'ID', 'OldFCID', 'OldOBJECTI',
    'Shape__Are', 'Shape__Len', 'Shape_Length', 'Shape_Area'
]

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
    # verify_gdf_columns(ifprs, IFPRS_COLUMNS, logger)

    # Filter to California and post-10/1/2023 records
    logger.info("   Filtering to California and Completion after 2023-10-01...")
    ifprs = ifprs[ifprs['State'] == 'California']
    logger.debug(f"      ifprs shape after filtering State by California: {ifprs.shape}")

    ifprs['Completion'] = pd.to_datetime(ifprs['CompletionDate'], errors='coerce', utc=True)
    ifprs = ifprs[ifprs['Completion'] >= pd.Timestamp('2023-10-01', tz='UTC')]
    ifprs = ifprs.to_crs(3310)
    logger.debug(f"      ifprs shape after filtering by 2023-10-01: {ifprs.shape}")
     
    logger.info("Performing Standardization...")

    logger.info("   step 1/18 Clip Features to California...")
    ifprs_clip = clip_to_california(ifprs, a_reference_gdb_path)
    logger.info(f"      ifprs shape after clip_to_california: {ifprs_clip.shape}")
    
    logger.info("   step 2/18 Repairing Geometry...")
    ifprs_clip = repair_geometries(ifprs_clip)
    logger.info(f"      ifprs shape after repairing geometry: {ifprs_clip.shape}")
    
    logger.info("   step 3/18 Adding Common Columns...")
    standardized_ifprs = add_common_columns(ifprs_clip)
    # show_columns(logger, standardized_ifprs, "standardized_ifprs")
    # logger.info(f"      ifprs shape after add_common_columns: {standardized_ifprs.shape}")
    
    logger.info("   step 4/18 Transferring Values...")
    standardized_ifprs["PROJECTID_USER"] = standardized_ifprs["Name"].astype(str)
    # standardized_ifprs["AGENCY"] = standardized_ifprs["Agency"]
    standardized_ifprs["AGENCY"] = "DOI"
    standardized_ifprs["ORG_ADMIN_p"] = standardized_ifprs["Agency"]
    standardized_ifprs["ORG_ADMIN_t"] = standardized_ifprs["Agency"]
    standardized_ifprs["ORG_ADMIN_a"] = standardized_ifprs["Agency"]
    standardized_ifprs["PROJECT_CONTACT"] = None
    standardized_ifprs["PROJECT_EMAIL"] = None
    standardized_ifprs["ADMINISTERING_ORG"] = standardized_ifprs["Agency"]
    standardized_ifprs["PROJECT_NAME"] = standardized_ifprs["Name"]
    standardized_ifprs["PRIMARY_FUNDING_SOURCE"] = "FEDERAL"
    standardized_ifprs["PRIMARY_FUNDING_ORG"] = standardized_ifprs["Agency"]
    standardized_ifprs["IMPLEMENTING_ORG"] = standardized_ifprs["Agency"]
    standardized_ifprs["ACTIVITY_NAME"] = standardized_ifprs["Name"]
    standardized_ifprs["BVT_USERD"] = "NO"

    # logger.debug("-"*70)
    # logger.debug(standardized_ifprs[["PROJECTID_USER", "PROJECT_NAME", "ACTIVITY_NAME"]])

    cols = ["AGENCY", "ORG_ADMIN_p", "ADMINISTERING_ORG", "IMPLEMENTING_ORG"]
    missing_rows = standardized_ifprs[standardized_ifprs[cols].isna().any(axis=1)]
    # logger.debug(missing_rows)
    
    logger.info("   step 5/18 Calculating Start and End Date...")
    def safe_date_convert(x):
        if pd.isna(x):
            return None
        try:
            return pd.to_datetime(x, errors='coerce', utc=True)
        except Exception as e:
            logger.error(f"      Problem converting date: {x}, Error: {e}")
            return None

    standardized_ifprs["ACTIVITY_START"] = standardized_ifprs["InitiationDate"].apply(safe_date_convert)
    standardized_ifprs["ACTIVITY_END"] = standardized_ifprs["Completion"].apply(safe_date_convert)

    # logger.debug("-"*70)
    # logger.debug(standardized_ifprs[["ACTIVITY_START", "ACTIVITY_END"]])

    logger.info("   step 6/18 Calculating Status...")
    def map_status(status):
        status_map = {
            'started': 'Active',
            'not started': 'Planned',
            'completed': 'Complete',
            'cancelled': 'Cancelled'
        }
        return status_map.get(str(status).lower(), 'Unknown')

    standardized_ifprs["ACTIVITY_STATUS"] = standardized_ifprs["Status"].apply(map_status)
    
    logger.info("   step 7/18 Activity Quantity...")
    def calculate_quantity(total_acres, gis_acres):
        if pd.isna(total_acres) or total_acres == 0:
            return gis_acres
        return total_acres

    standardized_ifprs["TotalAcres"] = standardized_ifprs["CalculatedArea"]
    standardized_ifprs["ACTIVITY_QUANTITY"] = standardized_ifprs.apply(
        lambda row: calculate_quantity(row["TotalAcres"], row["Shape_Area"] / 4046.86), axis=1
    )
    standardized_ifprs["ACTIVITY_QUANTITY"] = standardized_ifprs["ACTIVITY_QUANTITY"].astype(float)
    standardized_ifprs["ACTIVITY_UOM"] = "AC"

    # logger.debug("-"*70)
    # logger.debug(standardized_ifprs[["TotalAcres", "Shape_Area", "ACTIVITY_QUANTITY"]])

    logger.info("   step 8/18 Enter Column Values...")
    standardized_ifprs["ADMIN_ORG_NAME"] = standardized_ifprs["Agency"]
    standardized_ifprs["IMPLEM_ORG_NAME"] = standardized_ifprs["Agency"]
    standardized_ifprs["PRIMARY_FUND_SRC_NAME"] = "FEDERAL"
    standardized_ifprs["PRIMARY_FUND_ORG_NAME"] = standardized_ifprs["Agency"]
    standardized_ifprs["Source"] = "IFPRS"

    missing_rows = standardized_ifprs[standardized_ifprs[cols].isna().any(axis=1)]
    # logger.debug(missing_rows)
    
    standardized_ifprs["Year"] = standardized_ifprs["ACTIVITY_END"].apply(
        lambda x: x.year if pd.notnull(x) else None
    )

    logger.info("   step 9/18 Adding Activity Description to Crosswalk Column...")
    def crosswalk_activity(type_val, subtype_val, name, notes):
        
        type_val = str(type_val).lower() if pd.notnull(type_val) else ""
        subtype_val = str(subtype_val).lower() if pd.notnull(subtype_val) else ""
        name = str(name).lower() if pd.notnull(name) else ""
        notes = str(notes).lower() if pd.notnull(notes) else ""

        # Excel crosswalk mappings
        if type_val == "wildfire":
            return "NOT_DEFINED"
        elif type_val in ["broadcast", "underburn"]:
            return "BROADCAST_BURN"
        elif type_val == "pile burn":
            return "PILE_BURN"
        elif type_val == "thinning":
            return "THIN_MECH"
        elif type_val in ["clearcut, chaining", "fuel compaction crushing"]:
            return "CHAIN_CRUSH"
        elif type_val == "pile":
            return "PILING"
        elif type_val == "lop and scatter":
            return "LOP_AND_SCAT"
        elif type_val in ["fuel compaction grinding", "fuel compaction chipping",
                          "fuel compaction mastication", "grinding, chipping, crushing",
                          "fuel compaction"]:
            return "MASTICATION"
        elif type_val == "fuel compaction mowing":
            return "MOWING"
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

        # Additional rules based on Name and Notes
        if "pile" in name or "pile" in notes:
            return "PILE_BURN"
        elif "broadcast" in notes:
            return "BROADCAST_BURN"
        elif "hand" in notes:
            return "THIN_MAN"
        elif "chip" in notes:
            return "CHIPPING"
        elif "lop" in notes:
            return "LOP_AND_SCAT"
        elif "masticat" in notes:
            return "MASTICATION"
        elif "mow" in notes:
            return "MOWING"
        elif "biomass" in notes:
            return "BIOMASS_REMOVAL"

        return "NOT_DEFINED"

    standardized_ifprs["Crosswalk"] = standardized_ifprs.apply(
        lambda row: crosswalk_activity(row["Type"], row["SubType"], row["Name"], row["Notes"]), axis=1
    )
    # logger.debug(standardized_ifprs["Crosswalk"])

    logger.info("   step 10/18 Select by Years...")
    selected_gdf = standardized_ifprs[(standardized_ifprs["Year"] >= start_year) &
                                    (standardized_ifprs["Year"] <= end_year)]

    logger.info("   step 11/18 Create New GeoDataFrame Using the Template...")
    new_ifprs = gpd.GeoDataFrame(columns=get_wfr_tf_template(a_reference_gdb_path).columns, crs="EPSG:3310")

    logger.info("   step 12/18 Append to Template...")
    new_ifprs = pd.concat([new_ifprs, selected_gdf], ignore_index=True)

    logger.info("   step 13/18 Calculate Treatment Geometry...")
    new_ifprs["TRMT_GEOM"] = "POLYGON"

    logger.info("   step 14/18 Remove Unnecessary Columns...")
    new_ifprs = keep_fields(new_ifprs)
    # show_columns(logger, new_ifprs, "new_ifprs")

    logger.info("   step 15/18 Enriching Polygons...")
    enriched_ifprs = enrich_polygons(new_ifprs, a_reference_gdb_path, start_year, end_year)

    logger.info("   step 16/18 Calculate Treatment ID...")
    enriched_ifprs["TRMTID_USER"] = (
        enriched_ifprs["PROJECTID_USER"] + "-" +
        enriched_ifprs["COUNTY"].str[:3] + "-" +
        enriched_ifprs["PRIMARY_OWNERSHIP_GROUP"].str[:4] + "-" +
        enriched_ifprs["IN_WUI"].str[:3] + "-" +
        enriched_ifprs["PRIMARY_OBJECTIVE"].str[:8]
    )

    # show_columns(logger, enriched_ifprs, "enriched_ifprs")

    # check enriched_ifprs for missing data
    pd.set_option('display.max_rows', None)
    # logger.info("-"*70)
    # logger.info(enriched_ifprs[["PROJECTID_USER", "ORG_ADMIN_p", "ADMINISTERING_ORG", "PRIMARY_FUNDING_ORG"]])

    # Define the columns to check
    cols_to_check = ["PROJECTID_USER", "ORG_ADMIN_p", "ADMINISTERING_ORG", "PRIMARY_FUNDING_ORG"]

    # Filter rows where any of the specified columns are "N/A", "", or NaN
    mask = enriched_ifprs[cols_to_check].applymap(
        lambda val: pd.isna(val) or val == "" or val == "N/A" or val == 'City, County, Other' or val == 'BIA, Tribal' or val == 'State'
    ).any(axis=1)

    # Select those rows and the relevant columns
    rows_with_issues = enriched_ifprs.loc[mask, cols_to_check]

    # Log only if there are matching rows
    if not rows_with_issues.empty:
        # logger.info("="*70)
        # logger.info("Rows with 'N/A', empty, or missing values:\n%s", rows_with_issues)

        # Define the mapping
        fws_ids = [
            "CATNR-HFR-FY24-MX-W",
            "CASOR-HFR-FY24-RX-N",
            "CASOR-HFR-FY24-MX-N",
            "CALUR-HFR-FY24-MX-165 West Disking",
            "CALUR-HFR-FY24-MX-W-East Bear Creek 2",
            "CALUR-HFR-FY24_MX-KestersonMowing 2",
            "CALUR-HFR-FY24_MX-W-Kesterson Mowing1",
            "CAHBR-HFR-FY24_MX-W"
        ]

        nps_ids = [
            "NPS PWR SAMO FY24 Strategic Fuels - BIL",
            "CHIS FY24 Rehab and monitor burn pile areas - BIL",
            "NPS PWR YOSE FY24 Soupbowl - CCI"
        ]

        bia_ids = [
            "Oak Mountain Dozer Maintenance Fuel Break",
            "Water Plant Dozer line 1",
            "South Reservation Handline 1 Maintenance",
            "South Reservation Handline 2 Maintenance",
            "North Reservation Handline 1 Maintenance",
            "South Reservation Handline 4",
            "North Reservation Handline 2 Maintenance",
            "North Reservation Handline 3 Maintenance",
            "North Reservation Handline 4 Maintenance",
            "Blue Creek Dozer fuel Break",
            "BIL Toyon HFR Maintenance - Mowing"
        ]

        # Define the three target columns
        target_cols = ["ORG_ADMIN_p", "ADMINISTERING_ORG", "PRIMARY_FUNDING_ORG"]

        # Set values for FWS
        enriched_ifprs.loc[enriched_ifprs["PROJECTID_USER"].isin(fws_ids), target_cols] = "FWS"

        # Set values for NPS
        enriched_ifprs.loc[enriched_ifprs["PROJECTID_USER"].isin(nps_ids), target_cols] = "NPS"

        # Set values for BIA
        enriched_ifprs.loc[enriched_ifprs["PROJECTID_USER"].isin(bia_ids), target_cols] = "BIA"

        # logger.info("-"*70)
        # logger.info(enriched_ifprs[["PROJECTID_USER", "ORG_ADMIN_p", "ADMINISTERING_ORG", "PRIMARY_FUNDING_ORG"]])


    missing_rows = enriched_ifprs[enriched_ifprs[cols].isna().any(axis=1)]
    # rows_to_show = [9, 26, 69, 224, 321, 322]
    # print(enriched_ifprs.loc[rows_to_show][cols])
    
    logger.info("   step 17/18 Assign Domains...")
    enriched_ifprs = assign_domains(enriched_ifprs)
    
    missing_rows = enriched_ifprs[enriched_ifprs[cols].isna().any(axis=1)]
    missing_rows = missing_rows[cols]
    # logger.debug(missing_rows)     

    # Condition 1: IMPLEMENTING_ORG = 'State' and both fields are null
    cond_state = (
        (enriched_ifprs["IMPLEMENTING_ORG"] == "State") &
        (enriched_ifprs["ORG_ADMIN_p"].isna()) &
        (enriched_ifprs["ADMINISTERING_ORG"].isna())
    )
    enriched_ifprs.loc[cond_state, ["ORG_ADMIN_p", "ADMINISTERING_ORG", "PRIMARY_FUNDING_ORG", "AGENCY"]] = "OTHER"
    
    # Condition 2: IMPLEMENTING_ORG = 'BIA, Tribal' and both fields are null
    cond_bia = (
        (enriched_ifprs["IMPLEMENTING_ORG"] == "BIA, Tribal") &
        (enriched_ifprs["ORG_ADMIN_p"].isna()) &
        (enriched_ifprs["ADMINISTERING_ORG"].isna())
    )
    enriched_ifprs.loc[cond_bia, ["ORG_ADMIN_p", "ADMINISTERING_ORG"]] = "BIA"

    cond_bia_2 = (
        (enriched_ifprs["IMPLEMENTING_ORG"] == "BIA, Tribal") &
        (enriched_ifprs["PRIMARY_FUNDING_ORG"].isna())
    )
    enriched_ifprs.loc[cond_bia_2, ["PRIMARY_FUNDING_ORG"]] = "BIA"

    # Condition 3: IMPLEMENTING_ORG = 'USFS'
    cond_usfs = (
        (enriched_ifprs["IMPLEMENTING_ORG"] == "USFS")
    )
    enriched_ifprs.loc[cond_usfs, ["AGENCY"]] = "USDA"


    enriched_ifprs = assign_domains(enriched_ifprs)
    
    missing_rows = enriched_ifprs[enriched_ifprs[cols].isna().any(axis=1)]
    missing_rows = missing_rows[cols]
    # logger.debug(missing_rows)     
    
    logger.info("   step 18/18 Save Result...")
    save_gdf_to_gdb(enriched_ifprs,
                    output_gdb_path,
                    output_layer_name,
                    group_name="c_Enriched")

if __name__ == "__main__":
    # Get the current process ID
    process = psutil.Process(os.getpid())

    ifprs_input_gdb_path = "/tmp/IFPRS_20251010.gdb"
    ifprs_input_layer_name = "ifprs_actual_treatments_20251010"
    a_reference_gdb_path = "/home/klin/misc/test_its/a_Reference.gdb"
    start_year, end_year = 2023, 2025
    output_gdb_path = f"/tmp/IFPRS_{start_year}_{end_year}.gdb"
    output_layer_name = f"IFPRS_enriched_{datetime.today().strftime('%Y%m%d')}"

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
