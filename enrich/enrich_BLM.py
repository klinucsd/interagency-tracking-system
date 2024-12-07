
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


logger = logging.getLogger('enrich.enrich_BLM')

# Suppress pyogrio INFO logs
logging.getLogger("pyogrio").setLevel(logging.WARNING)

# Suppress specific warnings
warnings.filterwarnings("ignore", category=RuntimeWarning)

# Suppress specific FutureWarning messages
warnings.simplefilter(action='ignore', category=FutureWarning)


BLM_COLUMNS = [
    'UNIQUE_ID', 'SYS_CD', 'SYS_TRTMNT_ID', 'TRTMNT_NM', 'TRTMNT_TYPE_CD',
    'TRTMNT_SUBTYPE', 'TRTMNT_START_DT', 'TRTMNT_END_DT', 'TRTMNT_COMMENTS',
    'BLM_ACRES', 'GIS_ACRES', 'ADMIN_ST', 'Tmp_Text_ca', 'Tmp_Long_ca',
    'Tmp_Float_ca', 'Tmp_Date_ca', 'Comments_ca', 'CREATE_DATE', 'CREATE_BY',
    'MODIFY_DATE', 'MODIFY_BY', 'SHAPE_Length', 'SHAPE_Area', 'OBJECTID',
    'geometry'
]


def enrich_BLM(blm_gdb_path,
               blm_layer_name, 
               a_reference_gdb_path,
               start_year,
               end_year):

    logger.info("Load the BLM data into a GeoDataFrame")
    start = time.time()
    # blm = gpd.read_file(blm_gdb_path, driver="OpenFileGDB", layer=blm_layer_name)
    blm = gpd.read_file(blm_gdb_path, driver="OpenFileGDB", sql_dialect="OGRSQL", sql=f"SELECT *, OBJECTID FROM {blm_layer_name}")
    logger.info(f"   time for loading {blm_layer_name}: {time.time()-start}")
    
    # validate the input data
    verify_gdf_columns(blm, BLM_COLUMNS, logger)
    
    blm = blm.to_crs(3310)
    show_columns(logger, blm, "blm")

    logger.info("Performing Standardization...")

    logger.info("   step 1/15 Clip Features to California...")
    blm_clip = clip_to_california(blm, a_reference_gdb_path)
    
    logger.info("   step 2/15 Repairing Geometry...")
    blm_clip = repair_geometries(blm_clip)

    logger.info("   step 3/15 Adding Common Columns...")
    standardized_blm = add_common_columns(blm_clip)
    show_columns(logger, standardized_blm, "standardized_blm")
    
    logger.info("   step 4/15 Transfering Values...")
    standardized_blm["PROJECTID_USER"] = standardized_blm["UNIQUE_ID"]
    standardized_blm["AGENCY"] = "DOI"
    standardized_blm["ORG_ADMIN_p"] = "BLM"
    standardized_blm["ORG_ADMIN_t"] = "BLM"
    standardized_blm["ORG_ADMIN_a"] = "BLM"
    standardized_blm["PROJECT_CONTACT"] = None
    standardized_blm["PROJECT_EMAIL"] = None
    standardized_blm["ADMINISTERING_ORG"] = "BLM"
    standardized_blm["PROJECT_NAME"] = standardized_blm["TRTMNT_NM"]
    standardized_blm["PRIMARY_FUNDING_SOURCE"] = "FEDERAL"
    standardized_blm["PRIMARY_FUNDING_ORG"] = "NPS"
    standardized_blm["IMPLEMENTING_ORG"] = "BLM"
    standardized_blm["ACTIVITY_NAME"] = standardized_blm["TRTMNT_NM"]
    standardized_blm["BVT_USERD"] = "NO"

    logger.debug("-"*70)
    logger.debug(standardized_blm[["PROJECTID_USER", "PROJECT_NAME", "ACTIVITY_NAME"]])
    
    logger.info("   step 5/15 Calculating Start and End Date...")
    standardized_blm["ACTIVITY_START"] = standardized_blm["TRTMNT_START_DT"].apply(
        lambda x: datetime.strptime(x + '00', "%Y/%m/%d %H:%M:%S%z") if pd.notnull(x) else None
    )
    
    standardized_blm["ACTIVITY_END"] = standardized_blm["TRTMNT_END_DT"].apply(
        lambda x: datetime.strptime(x + '00', "%Y/%m/%d %H:%M:%S%z") if pd.notnull(x) else None
    )

    logger.debug("-"*70)
    logger.debug(standardized_blm[["ACTIVITY_START", "ACTIVITY_END"]])
    
    logger.info("   step 6/15 Calculating Status...")
    standardized_blm["ACTIVITY_STATUS"] = "COMPLETE"

    logger.info("   step 7/15 Activity Quantity...")
    def ifelse(BLM, GIS):
        if BLM == 0 or pd.isna(BLM):
            return GIS
        else:
            return BLM
        
    standardized_blm["ACTIVITY_QUANTITY"] = standardized_blm.apply(lambda row: ifelse(row["BLM_ACRES"], row["GIS_ACRES"]), axis=1)
    standardized_blm["ACTIVITY_QUANTITY"] = standardized_blm["ACTIVITY_QUANTITY"].astype(float)
    standardized_blm["ACTIVITY_UOM"] = "AC"

    logger.debug("-"*70)
    logger.debug(standardized_blm[["BLM_ACRES", "GIS_ACRES", "ACTIVITY_QUANTITY"]])
    
    logger.info("   step 8/15 Enter Column Values...")
    standardized_blm["ADMIN_ORG_NAME"] = "BLM"
    standardized_blm["IMPLEM_ORG_NAME"] = "BLM"
    standardized_blm["PRIMARY_FUND_SRC_NAME"] = "FEDERAL"
    standardized_blm["PRIMARY_FUND_ORG_NAME"] = "BLM"
    standardized_blm["Source"] = "BLM"

    standardized_blm["Year"] = standardized_blm["ACTIVITY_END"].apply(
        lambda x: x.year if pd.notnull(x) else None
    )

    logger.info("   step 9/15 Adding Original Activity Description to Crosswalk Column...")  
    def crosswalk_rule_1(treatment_type, sub, cross):
        if (treatment_type == "BIOLOGICAL" or treatment_type == 1) and sub == "CLASSICAL":
            return "PRESCRB_HERBIVORY"
        if (treatment_type == "BIOLOGICAL" or treatment_type == 1) and sub == "NON-CLASSICAL":
            return "PRESCRB_HERBIVORY"
        if sub == "FERTILIZER":
            return "NOT_DEFINED"
        if sub == "PESTICIDE":
            return "PEST_CNTRL"
        if treatment_type == "PRESCRIBED FIRE" or treatment_type == 3:
            return "BROADCAST_BURN"
        if (treatment_type == "PHYSICAL" or treatment_type == 4) and sub == "OTHER":
            return "THIN_MECH"
        if (treatment_type == "PHYSICAL" or treatment_type == 4) and sub == "REMOVE":
            return "THIN_MECH"
        if (treatment_type == "PHYSICAL" or treatment_type == 4) and sub == "PLANT":
            return "HABITAT_REVEG"
        return cross

    standardized_blm["Crosswalk"] = standardized_blm.apply(
        lambda row: crosswalk_rule_1(row["TRTMNT_TYPE_CD"], row["TRTMNT_SUBTYPE"], row["Crosswalk"]), axis=1
    )

    def crosswalk_rule_2(Nm, treatment_type, sub, com, cross):
        if Nm is None:
            return cross
        elif (treatment_type == "PRESCRIBED FIRE" or treatment_type == 3) and "pile" in Nm.lower():
            return "PILE_BURN"
        elif (treatment_type == "PRESCRIBED FIRE" or treatment_type == 3) and "hp" in Nm.lower():
            return "PILE_BURN"
        elif (treatment_type == "PRESCRIBED FIRE" or treatment_type == 3) and "hand" in Nm.lower():
            return "PILE_BURN"
        elif (treatment_type == "PHYSICAL" or treatment_type == 4) and "road" in Nm.lower():
            return "ROAD_CLEAR"
        elif (treatment_type == "PHYSICAL" or treatment_type == 4) and "chip" in Nm.lower():
            return "CHIPPING"
        elif (treatment_type == "PHYSICAL" or treatment_type == 4) and "hand" in Nm.lower():
            return "THIN_MAN"
        elif (treatment_type == "PHYSICAL" or treatment_type == 4) and "masticat" in Nm.lower():
            return "MASTICATION"
        return cross

    standardized_blm["Crosswalk"] = standardized_blm.apply(
        lambda row: crosswalk_rule_2(row["TRTMNT_NM"], row["TRTMNT_TYPE_CD"], row["TRTMNT_SUBTYPE"], row["TRTMNT_COMMENTS"], row["Crosswalk"]), axis=1
    )

    def crosswalk_rule_3(Nm, treatment_type, sub, com, cross):
        if com is None:
            return cross
        elif (treatment_type == "PRESCRIBED FIRE" or treatment_type == 3) and "pile" in com.lower():
            return "PILE_BURN"
        elif (treatment_type == "PRESCRIBED FIRE" or treatment_type == 3) and "broadcast" in com.lower():
            return "BROADCAST_BURN"
        elif (treatment_type == "PHYSICAL" or treatment_type == 4) and "hand" in com.lower():
            return "THIN_MAN"
        elif (treatment_type == "PHYSICAL" or treatment_type == 4) and "chip" in com.lower():
            return "CHIPPING"
        elif (treatment_type == "PHYSICAL" or treatment_type == 4) and "lop" in com.lower():
            return "LOP_AND_SCAT"
        elif (treatment_type == "PHYSICAL" or treatment_type == 4) and "masticat" in com.lower():
            return "MASTICATION"
        elif (treatment_type == "PHYSICAL" or treatment_type == 4) and "mow" in com.lower():
            return "MOWING"
        elif (treatment_type == "PHYSICAL" or treatment_type == 4) and "biomass" in com.lower():
            return "BIOMASS_REMOVAL"
        elif (treatment_type == "PHYSICAL" or treatment_type == 4) and "machine pile" in com.lower():
            return "PILING"
        return cross

    standardized_blm["Crosswalk"] = standardized_blm.apply(
        lambda row: crosswalk_rule_3(row["TRTMNT_NM"], row["TRTMNT_TYPE_CD"], row["TRTMNT_SUBTYPE"], row["TRTMNT_COMMENTS"], row["Crosswalk"]), axis=1
    )
    
    logger.info("   step 10/15 Select by Years...")
    selected_gdf = standardized_blm[(standardized_blm["Year"] >= start_year) & (standardized_blm["Year"] <= end_year)]
    
    logger.info("   step 10/15 Create New GeoDataframe Using the Template...")
    new_blm = gpd.GeoDataFrame(columns=get_wfr_tf_template(a_reference_gdb_path).columns, crs="EPSG:3310")  

    logger.info("   step 10/15 Append to Template...")
    new_blm = pd.concat([new_blm, selected_gdf], ignore_index=True)

    logger.info("   step 10/15 Calculate Treatment Geometry...")
    new_blm["TRMT_GEOM"] = "POLYGON"

    logger.info("   step 11/15 Remove Unnecessary Columns...")
    new_blm = keep_fields(new_blm)
    show_columns(logger, new_blm, "new_blm")    

    logger.info("   step 12/15 Enriching Polygons...")
    enriched_blm = enrich_polygons(new_blm, a_reference_gdb_path, start_year, end_year)  

    logger.info("   step 13/15 Calculate Treatment ID...")
    enriched_blm["TRMTID_USER"] = (
        enriched_blm["PROJECTID_USER"].str[:7] + "-" +
        enriched_blm["COUNTY"].str[:3] + "-" +
        enriched_blm["PRIMARY_OWNERSHIP_GROUP"].str[:4] + "-" +
        enriched_blm["IN_WUI"].str[:3] + "-" +
        enriched_blm["PRIMARY_OBJECTIVE"].str[:8]
    )

    show_columns(logger, enriched_blm, "enriched_blm")
    
    logger.info("   step 14/15 Assign Domains...")
    enriched_blm = assign_domains(enriched_blm)

    logger.info("   step 15/15 Save Result...")
    save_gdf_to_gdb(enriched_blm, f"/tmp/BLM_{start_year}_{end_year}.gdb", f"BLM_enriched_{datetime.today().strftime('%Y%m%d')}", group_name="c_Enriched")
    
    
if __name__ == "__main__":
    enrich_BLM("/home/klin/misc/test_its/BLM.gdb",
               "BLM_20230813",
               "a_Reference.gdb",
               2010,
               2025)

