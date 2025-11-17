
"""
# Description: Converts the U.S. Forest Service EDW FACTS Common Attributes dataset 
#              into the Task Force standardized schema.  Dataset
#              is enriched with vegetation, ownership, county, WUI, 
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

from its_logging.logger_config import logger
from utils.its_utils import clip_to_california, get_wfr_tf_template
from utils.gdf_utils import repair_geometries, show_columns, verify_gdf_columns
from utils.add_common_columns import add_common_columns
from utils.enrich_polygons import enrich_polygons
from utils.keep_fields import keep_fields
from utils.assign_domains import assign_domains
from utils.save_gdf_to_gdb import save_gdf_to_gdb


logger = logging.getLogger('enrich.enrich_USFS')

# Suppress pyogrio INFO logs
logging.getLogger("pyogrio").setLevel(logging.WARNING)

# Suppress specific warnings
warnings.filterwarnings("ignore", category=RuntimeWarning)

# Suppress specific FutureWarning messages
warnings.simplefilter(action='ignore', category=FutureWarning)


USFS_COLUMNS = [
    'SU_SECURITY_ID', 'AU_ORG', 'SUID', 'FACTS_ID', 'SUBUNIT', 'AU_NAME', 'NAME',
    'FEATURE_TYPE', 'SITE_NBR_OF_UNITS', 'GIS_ACRES', 'UOM', 'ACTIVITY_CODE',
    'ACTIVITY', 'LOCAL_QUALIFIER', 'METHOD_CODE', 'METHOD', 'EQUIPMENT_CODE',
    'EQUIPMENT', 'FUND_CODES', 'COST_PER_UNIT', 'WORKFORCE_CODE',
    'FISCAL_YEAR_PLANNED', 'FISCAL_YEAR_AWARDED', 'DATE_AWARDED',
    'FISCAL_YEAR_COMPLETED', 'DATE_COMPLETED', 'NBR_UNITS_PLANNED',
    'NBR_UNITS_ACCOMPLISHED', 'EXCLUDE_ACCOMPLISHMENT', 'TREATMENT_NAME',
    'FUELS_KEYPOINT_AREA', 'ISWUI', 'FIREREGIME', 'CWPP', 'BIOMASS_UTILIZATION',
    'NFPORS_CATEGORY', 'NFPORS_TREATMENT', 'PURPOSE_CODE', 'SALE_NAME', 'SALE_NUMBER',
    'SALE_CATEGORY', 'UNIT_ID', 'PURCHASER_NAME', 'CONTRACT_PLANNED_TERM',
    'AWARD_DATE', 'SALE_CLOSURE_DATE', 'BASE_YEAR', 'KV_NBR_UNITS_FUNDED',
    'PERCENT_FUNDED', 'NEEDS', 'CAUSAL_AGENT', 'REFORESTATION_STATUS', 'EXAM_NBR',
    'NEEDS_ADJUSTMENT', 'TSI', 'EVENT_YEAR', 'IMPLEMENTATION_PROJECT',
    'IMPL_PROJECT_NBR', 'IMPL_PROJECT_TYPE', 'NEPA_DOC_NBR', 'NEPA_DOC_TYPE',
    'NEPA_PROJECT_NAME', 'NEPA_HFI', 'NEPA_HFRA', 'NEPA_SIGNED_DATE',
    'ADMIN_REGION', 'ADMIN_FOREST', 'ADMIN_DISTRICT', 'AU_REGION', 'AU_FOREST',
    'AU_DISTRICT', 'PROC_FOREST', 'OWNERSHIP', 'STATE_ABBR', 'PRODUCTIVITY_CLASS',
    'LAND_SUITABILITY_CODE', 'COUNTY_NAME', 'CONG_DIST_NAME', 'LATITUDE', 'LONGITUDE',
    'LEGAL_LOCATION', 'ASPECT', 'SLOPE', 'ELEVATION', 'WATERSHED_CODE', 'MGT_AREA_CODE',
    'MGT_PRESCRIPTION_CODE', 'ACTIVITY_SITE_REMARKS', 'ACTIVITY_REMARKS',
    'SU_CREATED_BY', 'SU_CREATED_DATE', 'SU_MODIFIED_BY', 'SU_MODIFIED_DATE',
    'ACT_CREATED_BY', 'ACT_CREATED_DATE', 'ACT_MODIFIED_BY', 'ACT_MODIFIED_DATE',
    'ACTIVITY_UNIT_CN', 'LU_CN', 'SUID_CN', 'EVENT_CN', 'NEPA_PROJECT_CN',
    'PALS_PROJECT_CN', 'SALE_CN', 'IMPLEMENTATION_PROJECT_CN', 'UKCN', 'FS_UNIT_ID',
    'CRC_VALUE', 'EVENT_NAME', 'SHAPE_Length', 'SHAPE_Area', 'geometry']

def enrich_USFS(usfs_gdb_path,
                usfs_layer_name, 
                a_reference_gdb_path,
                start_year,
                end_year,
                output_gdb_path,
                output_layer_name,
                manager = None):

    usfs_gdb_name = os.path.basename(usfs_gdb_path)        
    logger.info(f"Loading the USFS data into GeoDataFrames: {usfs_gdb_name} : {usfs_layer_name}")
    start = time.time()

    if not os.path.exists("cache"):
        os.makedirs("cache")

    usfs_layer = f"{usfs_gdb_name}_{usfs_layer_name}"
    if os.path.exists(f"cache/{usfs_layer}.parquet"):
        logger.info("   Loading USFS data from cache")
        usfs = gpd.read_parquet(f"cache/{usfs_layer}.parquet")
    else:
        logger.info("   Loading USFS data from source and cache the data")
        usfs = gpd.read_file(usfs_gdb_path, driver="OpenFileGDB", sql_dialect="OGRSQL", sql=f"SELECT *, OBJECTID FROM {usfs_layer_name}")
        usfs.to_parquet(f"cache/{usfs_layer}.parquet")

    logger.info(f"      records: {usfs.shape[0]}")
    logger.info(f"      time for loading USFS: {time.time()-start}")

    # validate the input data
    verify_gdf_columns(usfs, USFS_COLUMNS, logger)
    
    usfs = usfs.to_crs(3310)
    show_columns(logger, usfs, "usfs")

    logger.info("Performing Standardization...")

    # Filter rows with None geometries
    none_geometry_rows = usfs[usfs['geometry'].isna()]
    logger.info(f"   found {none_geometry_rows.shape[0]} rows with empty geometry")
    logger.info(f"   drop {none_geometry_rows.shape[0]} rows with empty geometry")
    usfs = usfs.dropna(subset=['geometry'])
    
    # Filter for California records using boolean indexing
    usfs = usfs[usfs['STATE_ABBR'] == 'CA'].copy()
    logger.info(f"   records in California: {usfs.shape[0]}")

    logger.info("   step 1/8 Selecting Features...")
    
    # Initial activity code selection
    activity_codes = ['1102', '1111', '1112', '1113', '1115', '1116', '1117', '1118', '1119', 
                     '1120', '1130', '1136', '1139', '1150', '1152', '1153', '1154', '1160', 
                     '1180', '2000', '2341', '2360', '2370', '2510', '2530', '2540', '2560', 
                     '3132', '4101', '4102', '4111', '4113', '4115', '4117', '4121', '4122', 
                     '4131', '4132', '4141', '4142', '4143', '4145', '4146', '4148', '4151', 
                     '4152', '4162', '4175', '4177', '4183', '4192', '4193', '4194', '4196', 
                     '4210', '4211', '4220', '4231', '4232', '4241', '4242', '4250', '4270', 
                     '4280', '4290', '4291', '4382', '4411', '4412', '4431', '4432', '4455', 
                     '4471', '4472', '4473', '4474', '4475', '4481', '4482', '4483', '4484', 
                     '4485', '4490', '4491', '4492', '4493', '4494', '4495', '4511', '4521', 
                     '4530', '4540', '4541', '4550', '4580', '6101', '6103', '6104', '6105', 
                     '6106', '6107', '6133', '6584', '6684', '7015', '7050', '7065', '7067', 
                     '9008', '9400']
    
    gdf = usfs[usfs['ACTIVITY_CODE'].isin(activity_codes)].copy()
    
    # Filter out specific conditions for activity codes

    main_gdf = gdf[~gdf['ACTIVITY_CODE'].isin(['1117', '1119', '2510', '2341'])]
    # only keep 1117 (Wildfire - natural ignition) where keypoint is 6 (fuels reduction program)

    select_1117_6 = gdf[(gdf['ACTIVITY_CODE'] == '1117') & (gdf['FUELS_KEYPOINT_AREA'] == '6')]

    # only keep 1119 (Planned Treatment Burned in Wildfire) where keypoint is 6 (fuels reduction program)
    select_1119_6 = gdf[(gdf['ACTIVITY_CODE'] == '1119') & (gdf['FUELS_KEYPOINT_AREA'] == '6')]

    # only keep 2510 where keypoint is 6 or 3
    select_2150_6 = gdf[(gdf['ACTIVITY_CODE'] == '2510') & (gdf['FUELS_KEYPOINT_AREA'] == '6')]
    select_2150_3 = gdf[(gdf['ACTIVITY_CODE'] == '2510') & (gdf['FUELS_KEYPOINT_AREA'] == '3')]

    # only keep 2341 where keypoint is 6 or 3
    select_2341_6 = gdf[(gdf['ACTIVITY_CODE'] == '2341') & (gdf['FUELS_KEYPOINT_AREA'] == '6')]
    select_2341_3 = gdf[(gdf['ACTIVITY_CODE'] == '2341') & (gdf['FUELS_KEYPOINT_AREA'] == '3')]
    

    gdf = pd.concat([main_gdf, select_1117_6, select_1119_6, select_2150_3, select_2150_6, select_2341_3, select_2341_6])
    
    # Date filtering
    has_date = ~(gdf['DATE_COMPLETED'].isna() & gdf['DATE_AWARDED'].isna() & gdf['NEPA_SIGNED_DATE'].isna())
    
    # Create timezone-aware timestamp for comparison
    start_date = pd.Timestamp(f"{start_year}-01-01", tz='UTC')
    date_after_1995 = (gdf['DATE_COMPLETED'].fillna(pd.Timestamp.max.tz_localize('UTC')) >= start_date)
    
    gdf = gdf[has_date & date_after_1995]
    
    logger.info(f"      selected Activities have {len(gdf)} records")
    
    logger.info("   step 2/8 Repairing Geometry...")
    gdf = repair_geometries(gdf)
    
    logger.info("   step 3/8 Adding Fields...")
    # Rename fields
    gdf = gdf.rename({
        'TREATMENT_NAME': 'TREATMENT_NAME_FACTS',
        'LATITUDE': 'LATITUDE_',
        'LONGITUDE': 'LONGITUDE_'
    })
    gdf = add_common_columns(gdf)
    
    logger.info("   step 4/8 Transfering Attributes...")
    # Add new fields with constant values
    gdf['PROJECTID_USER'] = 'USFS-' + gdf['NEPA_DOC_NBR'].astype(str)
    gdf['AGENCY'] = 'USDA'
    gdf['ORG_ADMIN_p'] = 'USFS'
    gdf['ORG_ADMIN_t'] = 'USFS'
    gdf['ORG_ADMIN_a'] = 'USFS'
    gdf['PROJECT_CONTACT'] = 'Tawndria Melville'
    gdf['PROJECT_EMAIL'] = 'tawndria.melville@usda.gov'
    gdf['ADMINISTERING_ORG'] = 'USFS'
    gdf['PRIMARY_FUNDING_SOURCE'] = 'FEDERAL'
    gdf['PRIMARY_FUNDING_ORG'] = 'USFS'
    gdf['IMPLEMENTING_ORG'] = 'Pacific Southwest Regional Office'
    gdf['TRMTID_USER'] = gdf['SUID']
    gdf['ACTIVID_USER'] = gdf['SUID'].astype(str) + '-' + gdf['OBJECTID'].astype(str)
    gdf['BVT_USERD'] = 'NO'
    
    # WUI calculation
    gdf['IN_WUI'] = gdf['ISWUI'].map({'Y': 'WUI_USER_DEFINED', 'N': 'NON-WUI_USER_DEFINED'})
    
    logger.info("   step 5/8 Calculating End Date...")
    gdf['ACTIVITY_END'] = gdf['DATE_COMPLETED']
    gdf.loc[gdf['ACTIVITY_END'].isna(), 'ACTIVITY_END'] = gdf['NEPA_SIGNED_DATE']
    

    # TODO this need to be input year specific
    logger.info("   step 6/8 Calculating Status...")
    def get_status(row):
        if pd.notnull(row['DATE_COMPLETED']):
            return 'COMPLETE'
        elif pd.notnull(row['DATE_AWARDED']):
            return 'ACTIVE'
        elif row['NEPA_SIGNED_DATE'] >= pd.Timestamp('2025-01-24', tz='UTC'):
            return 'OUTYEAR'
        elif row['NEPA_SIGNED_DATE'] >= pd.Timestamp('2014-01-24', tz='UTC'):
            return 'PLANNED'
        else:
            return 'CANCELLED'
    
    gdf['ACTIVITY_STATUS'] = gdf.apply(get_status, axis=1)
    
    logger.info("   step 7/8 Activity Quantity...")
    gdf['ACTIVITY_QUANTITY'] = gdf['NBR_UNITS_ACCOMPLISHED'].fillna(gdf['NBR_UNITS_PLANNED'])
    gdf['ACTIVITY_UOM'] = gdf['UOM']
    
    logger.info("   step 8/8 Enter Field Values...")
    gdf['ADMIN_ORG_NAME'] = 'USFS'
    gdf['IMPLEM_ORG_NAME'] = gdf['WORKFORCE_CODE']
    gdf['PRIMARY_FUND_SRC_NAME'] = 'USFS'
    gdf['PRIMARY_FUND_ORG_NAME'] = 'USFS'
    gdf['ACTIVITY_NAME'] = None
    gdf['Source'] = 'usfs_treatments'
    gdf['Year'] = gdf['ACTIVITY_END'].dt.year
    
    # Handle special case for activity name
    gdf['Crosswalk'] = gdf['ACTIVITY'].apply(
        lambda x: 'Piling of Fuels, Hand or Machine' if x == 'Piling of Fuels, Hand or Machine ' else x
    )
    
    # Set treatment geometry type
    def get_geom_type(geom):
        if geom == 'A':
            return 'POLYGON'
        elif geom == 'L':
            return 'LINE'
        elif geom == 'P':
            return 'POINT'
        return 'POLYGON'
    
    gdf['TRMT_GEOM'] = gdf['ACTIVITY'].apply(get_geom_type)
    gdf['Act_Code'] = gdf['ACTIVITY_CODE'].astype('int64')

    logger.info("Remove Unnecessary Columns...")
    gdf = keep_fields(gdf)

    #this line is probably not required in the current 
    logger.info(f"Select records between {start_year} and {end_year}...")
    gdf_filtered = gdf[(gdf['Year'] >= start_year) & (gdf['Year'] <= end_year)]

    logger.info("Enriching Dataset...")
    enriched_gdf = enrich_polygons(gdf_filtered, a_reference_gdb_path, start_year, end_year, manager=manager)

    logger.info("Assign Domains...")
    enriched_gdf = assign_domains(enriched_gdf)

    logger.info("Save Result...")
    save_gdf_to_gdb(enriched_gdf,
                    output_gdb_path,
                    output_layer_name,
                    group_name="c_Enriched")
    
    
if __name__ == "__main__":
    # Get the current process ID
    process = psutil.Process(os.getpid())


    # load config file path yaml
    with open("..\config.yaml", 'r') as stream:
        config_inputs = yaml.safe_load(stream)

    usfs_input_base_path = config_inputs['sources']['usfs']['input']['base_path'] 
    a_reference_gdb_path = config_inputs['global']['reference_gdb']
    start_year, end_year = config_inputs['global']['start_year'], config_inputs['global']['end_year']
    output_format_dict = {'start_year': start_year,
                          'end_year': end_year,
                          'date': datetime.today().strftime('%Y%m%d')}
    output_gdb_path = config_inputs['sources']['usfs']['output']['gdb_path'].format(**output_format_dict)

    region_ids = config_inputs['sources']['usfs']['input']['regions']
    for region_id in region_ids: 
        usfs_input_file_name = config_inputs['sources']['usfs']['input']['gdb_template'].format(**{'region': region_id})
        usfs_input_gdb_path = os.path.join(usfs_input_base_path, usfs_input_file_name)
        usfs_input_layer_name = config_inputs['sources']['usfs']['input']['layer_name']
        output_format_dict['region'] = region_id
        output_layer_name = config_inputs['sources']['usfs']['output']['layer_name'].format(**output_format_dict)
        enrich_USFS(usfs_input_gdb_path,
                    usfs_input_layer_name,
                    a_reference_gdb_path,
                    start_year,
                    end_year,
                    output_gdb_path,
                    output_layer_name)

    # Get memory usage in bytes, convert to MB
    memory_usage = process.memory_info().rss / 1024 / 1024
    logger.info(f"Memory usage: {memory_usage:.2f} MB")
    




    
