
"""
# Description: Converts the U.S. Department of Interior, National 
#              Park Service's fuels treatments dataset 
#              into the Task Force standardized schema.  Dataset
#              is enriched with vegetation, ownership, county, WUI, 
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
from utils.gdf_utils import repair_geometries, show_columns, verify_gdf_columns, fetch_arcgis_feature_service
from utils.add_common_columns import add_common_columns
from utils.enrich_polygons import enrich_polygons
from utils.keep_fields import keep_fields
from utils.assign_domains import assign_domains
from utils.save_gdf_to_gdb import save_gdf_to_gdb


logger = logging.getLogger('enrich.enrich_NPS')

# Suppress pyogrio INFO logs
logging.getLogger("pyogrio").setLevel(logging.WARNING)

# Suppress specific warnings
warnings.filterwarnings("ignore", category=RuntimeWarning)

# Suppress specific FutureWarning messages
warnings.simplefilter(action='ignore', category=FutureWarning)


NPS_COLUMNS = [
    'OBJECTID', 'TreatmentID', 'LocalTreatmentID', 'TreatmentIdentifierDatabase',
    'NWCGUnitID', 'ProjectID', 'TreatmentName', 'TreatmentCategory', 'TreatmentType',
    'ActualCompletionDate', 'ActualCompletionFiscalYear', 'TreatmentAcres', 'GISAcres',
    'TreatmentStatus', 'TreatmentNotes', 'DateCurrent', 'PublicDisplay', 'DataAccess',
    'UnitCode', 'UnitName', 'GroupCode', 'GroupName', 'RegionCode', 'CreateDate',
    'CreateUser', 'LastEditDate', 'LastEditor', 'MapMethod', 'MapSource', 'SourceDate',
    'XYAccuracy', 'Notes', 'EventID', 'geometry'
]


def dissolve_with_nulls(gdf, dissolve_fields, integer_fields=None):
    """
    Perform dissolve operation while properly handling null values and data types.
    
    Args:
        gdf: GeoDataFrame to dissolve
        dissolve_fields: List of fields to dissolve by
        integer_fields: List of fields that should remain as integers
    """
    # Create a copy to avoid modifying the original
    work_gdf = gdf.copy()
    
    # Track original dtypes
    original_dtypes = {field: work_gdf[field].dtype for field in dissolve_fields}
    
    for field in dissolve_fields:
        if field not in work_gdf.columns:
            logger.warning(f"Field {field} not found in the GeoDataFrame")
            dissolve_fields.remove(field)
            continue
            
        dtype = work_gdf[field].dtype
        
        if pd.api.types.is_numeric_dtype(dtype):
            work_gdf[field] = work_gdf[field].fillna(-999999)
        elif pd.api.types.is_datetime64_dtype(dtype):
            work_gdf[field] = work_gdf[field].fillna(pd.Timestamp.min)
        else:
            work_gdf[field] = work_gdf[field].fillna('NONE')
    
    dissolved = work_gdf.dissolve(by=dissolve_fields, as_index=False)
    
    for field in dissolve_fields:
        if field not in dissolved.columns:
            continue
            
        dtype = dissolved[field].dtype
        if pd.api.types.is_numeric_dtype(dtype):
            dissolved.loc[dissolved[field] == -999999, field] = pd.NA
            # Restore integer type if specified and no nulls exist
            if integer_fields and field in integer_fields:
                if not dissolved[field].isna().any():
                    dissolved[field] = dissolved[field].astype(original_dtypes[field])
        elif pd.api.types.is_datetime64_dtype(dtype):
            dissolved.loc[dissolved[field] == pd.Timestamp.min, field] = pd.NaT
        else:
            dissolved.loc[dissolved[field] == 'NONE', field] = pd.NA
    
    return dissolved


def enrich_NPS(nps,
               a_reference_gdb_path,
               start_year,
               end_year,
               output_gdb_path,
               output_layer_name,):
    
    logger.info("Performing Standardization...")
    logger.info("   step 1/11 select after 1995")
    
    # Convert ActualCompletionDate to datetime if it's not already
    nps['ActualCompletionDate'] = pd.to_datetime(nps['ActualCompletionDate'], unit='ms')
    
    # Filter dates after 1995
    mask = (nps['ActualCompletionDate'] > '1995-01-01') | (nps['ActualCompletionDate'].isna())
    selected_gdf = nps[mask].copy()

    logger.info("   step 2/11 repairing geometry")
    selected_gdf = repair_geometries(selected_gdf)
    show_columns(logger, selected_gdf, "selected_gdf")

    logger.info("   step 3/11 clip features by CA")
    nps_clip = clip_to_california(selected_gdf, a_reference_gdb_path)
    show_columns(logger, nps_clip, "nps_clip")
    
    logger.info("   step 4/11 dissolve to implement multipart polygons")
    dissolve_fields = [
        "TreatmentID", "LocalTreatmentID", "TreatmentIdentifierDatabase",  "NWCGUnitID",  "ProjectID",  "TreatmentName",  "TreatmentCategory",
        "TreatmentType",  "ActualCompletionDate",  "ActualCompletionFiscalYear",  "TreatmentAcres",  "GISAcres",  "TreatmentStatus",
        "TreatmentNotes",  "DateCurrent",  "PublicDisplay",  "DataAccess",  "UnitCode",  "UnitName",  "GroupCode",  "GroupName",  "RegionCode",
        "CreateDate",  "CreateUser",  "LastEditDate",  "LastEditor",  "MapMethod",  "MapSource",  "SourceDate",  "XYAccuracy",  "Notes",  "EventID"
    ]

    integer_fields = ["ProjectID", "ActualCompletionFiscalYear", "TreatmentAcres", "GISAcres", "DateCurrent", "CreateDate", "LastEditDate", "SourceDate"]
    dissolved_gdf = dissolve_with_nulls(nps_clip, dissolve_fields, integer_fields)
    show_columns(logger, dissolved_gdf, "dissolved_gdf")
    
    logger.info("   step 5/11 rename and add fields")
    dissolved_gdf = dissolved_gdf.rename(columns={'ProjectID': 'PrjID'})
    show_columns(logger, dissolved_gdf, "dissolved_gdf")

    standardized_nps = add_common_columns(dissolved_gdf)
    show_columns(logger, standardized_nps, "standardized_nps")
    
    logger.info("   step 6/11 import attributes")

    standardized_nps['PROJECTID_USER'] = standardized_nps.apply(
        lambda row: row['PrjID'] if pd.notna(row['PrjID']) else row['TreatmentID'],
        axis=1
    )
    
    # Static field assignments
    standardized_nps['AGENCY'] = 'DOI'
    standardized_nps['ORG_ADMIN_p'] = 'NPS'
    standardized_nps['PROJECT_CONTACT'] = 'Kent van Wagtendonk'
    standardized_nps['PROJECT_EMAIL'] = 'Kent_Van_Wagtendonk@nps.gov'
    standardized_nps['PRIMARY_FUNDING_SOURCE'] = 'NPS'
    standardized_nps['PRIMARY_FUNDING_ORG'] = 'OTHER'
    standardized_nps['BVT_USERD'] = 'NO'
    standardized_nps['ACTIVITY_UOM'] = 'AC'
    standardized_nps['ADMIN_ORG_NAME'] = 'NPS'
    standardized_nps['PRIMARY_FUND_SRC_NAME'] = 'NPS'
    standardized_nps['PRIMARY_FUND_ORG_NAME'] = 'NPS'
    standardized_nps['Source'] = 'nps_flat_fuelstreatments'
    
    # Field assignments from existing columns
    standardized_nps['ADMINISTERING_ORG'] = standardized_nps['UnitCode']
    standardized_nps['PROJECT_NAME'] = standardized_nps['TreatmentName']
    standardized_nps['IMPLEMENTING_ORG'] = standardized_nps['UnitName']
    standardized_nps['PROJECTNAME_'] = standardized_nps['TreatmentName']
    standardized_nps['ORG_ADMIN_t'] = None
    standardized_nps['IMPLEM_ORG_NAME'] = standardized_nps['UnitName']
    standardized_nps['ACTIVITY_QUANTITY'] = standardized_nps['TreatmentAcres']
    
    def calculate_activity_end(row):
        if pd.notna(row['ActualCompletionDate']):
            return row['ActualCompletionDate']
        elif pd.notna(row['ActualCompletionFiscalYear']):
            return datetime(row['ActualCompletionFiscalYear'], 10, 1)
        return None
    
    standardized_nps['ACTIVITY_END'] = standardized_nps.apply(calculate_activity_end, axis=1)
    
    def calculate_activity_status(status):
        if status == "Completed":
            return "COMPLETE"
        elif status == "Initiated":
            return "ACTIVE"
        return "TBD"
    
    standardized_nps['ACTIVITY_STATUS'] = standardized_nps['TreatmentStatus'].apply(calculate_activity_status)
    
    # Extract year from ActualCompletionDate
    standardized_nps['Year'] = pd.to_datetime(standardized_nps['ActualCompletionDate']).dt.year
    
    def calculate_crosswalk(row):
        if pd.notna(row['TreatmentType']):
            return row['TreatmentType']
        elif pd.isna(row['TreatmentType']) and row['TreatmentCategory'] == "Fire":
            return "Broadcast Burn"
        elif pd.isna(row['TreatmentType']) and row['TreatmentCategory'] == "Mechanical":
            return "Hand Pile Burn"
        return None
    
    standardized_nps['Crosswalk'] = standardized_nps.apply(calculate_crosswalk, axis=1)
    show_columns(logger, standardized_nps, "standardized_nps")    
    
    filtered_gdf = standardized_nps[(standardized_nps['Year'] >= start_year) & (standardized_nps['Year'] <= end_year)]
    show_columns(logger, filtered_gdf, "filtered_gdf")    

    logger.info("   step 7/11 Remove Unnecessary Columns...")    
    filtered_gdf = keep_fields(filtered_gdf)

    logger.info("   step 8/11 Enriching Polygons...")
    enriched_nps = enrich_polygons(filtered_gdf, a_reference_gdb_path, start_year, end_year)  

    logger.info("   step 9/11 adding treatment ID")

    def safe_slice(value, length):
        """Safely slice a value, handling None and empty values"""
        if pd.isna(value) or value == '':
            return ''
        return str(value)[:length]

    def create_treatment_id(row):
        parts = [
            safe_slice(row['PROJECTID_USER'], 7),
            safe_slice(row['COUNTY'], 3),
            safe_slice(row['PRIMARY_OWNERSHIP_GROUP'], 4),
            safe_slice(row['IN_WUI'], 3),
            safe_slice(row['PRIMARY_OBJECTIVE'], 8)
        ]
        # Only join if there are non-empty parts
        non_empty_parts = [part for part in parts if part]
        if not non_empty_parts:  # If all parts are empty
            return None  # or return '' if you prefer empty string
        return '-'.join(non_empty_parts)

    # Apply the function to create treatment IDs
    enriched_nps['TRMTID_USER'] = enriched_nps.apply(create_treatment_id, axis=1)
    
    logger.info("   step 10/11 Assign Domains...")
    enriched_nps = assign_domains(enriched_nps)
    
    logger.info("   step 11/11 Save Result...")
    save_gdf_to_gdb(enriched_nps,
                    output_gdb_path,
                    output_layer_name,
                    group_name="c_Enriched")



def enrich_NPS_from_gdb(nps_gdb_path,
                        nps_layer_name, 
                        a_reference_gdb_path,
                        start_year,
                        end_year,
                        output_gdb_path,
                        output_layer_name):

    logger.info("Load the NPS data into a GeoDataFrame")
    start = time.time()

    nps = gpd.read_file(nps_gdb_path, driver="OpenFileGDB", sql_dialect="OGRSQL", sql=f"SELECT *, OBJECTID FROM {nps_layer_name}")
    logger.info(f"   time for loading {nps_layer_name}: {time.time()-start}")
    
    # validate the input data
    verify_gdf_columns(nps, NPS_COLUMNS, logger)
    nps = nps.to_crs(3310)
    show_columns(logger, nps, "nps")

    enrich_NPS(nps, a_reference_gdb_path, start_year, end_year, output_gdb_path, output_layer_name)


def enrich_NPS_from_arcgis(nps_feature_layer_url,
                           a_reference_gdb_path,
	                   start_year,
                           end_year,
                           output_gdb_path,
                           output_layer_name,):
    
    logger.info("Load the NPS data into a GeoDataFrame")
    start = time.time()

    nps = fetch_arcgis_feature_service(nps_feature_layer_url)
    logger.info(f"   time for loading NPS data: {time.time()-start}")

    # validate the input data                                                                                                                                      
    verify_gdf_columns(nps, NPS_COLUMNS, logger)
    nps = nps.to_crs(3310)
    show_columns(logger, nps, "nps")

    enrich_NPS(nps, a_reference_gdb_path, start_year, end_year, output_gdb_path, output_layer_name)

    
    
if __name__ == "__main__":

    # Get the current process ID
    process = psutil.Process(os.getpid())

    nps_gdb_path = 'New_NPS_2023_20240625_ReisThomasViaUpload_1.gdb'
    nps_layer_name = 'NPS_2023_20240625_ReisThomasViaUpload2'
    nps_arcgis_feature_url = None
    # nps_arcgis_feature_url = "https://services3.arcgis.com/T4QMspbfLg3qTGWY/ArcGIS/rest/services/s_Completed_Perimeters_Past_5FY_View/FeatureServer/0"
    a_reference_gdb_path = "a_Reference.gdb"
    start_year, end_year = 2010, 2025
    output_gdb_path = f"/tmp/NPS_{start_year}_{end_year}.gdb"
    output_layer_name = f"NPS_enriched_{datetime.today().strftime('%Y%m%d')}"

    if nps_arcgis_feature_url:
        enrich_NPS_from_arcgis(nps_arcgis_feature_url,
                               a_reference_gdb_path,
                               start_year,
                               end_year,
                               output_gdb_path,
                               output_layer_name)
    else:
        enrich_NPS_from_gdb(nps_gdb_path,
                            nps_layer_name,
                            a_reference_gdb_path,
                            start_year,
                            end_year,
                            output_gdb_path,
                            output_layer_name)

    # Get memory usage in bytes, convert to MB
    memory_usage = process.memory_info().rss / 1024 / 1024
    logger.info(f"Memory usage: {memory_usage:.2f} MB")


    
