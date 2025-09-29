
"""
# Description: Converts the California Department of Environmental Quality's 
#              Prescribed Fire Information Reporting System (PFIRS) dataset 
#              of Land Management's fuels treatments dataset 
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
from utils.enrich_points import enrich_points
from utils.keep_fields import keep_fields
from utils.assign_domains import assign_domains
from utils.save_gdf_to_gdb import save_gdf_to_gdb


logger = logging.getLogger('enrich.enrich_PFIRS')

# Suppress pyogrio INFO logs
logging.getLogger("pyogrio").setLevel(logging.WARNING)

# Suppress specific warnings
warnings.filterwarnings("ignore", category=RuntimeWarning)

# Suppress specific FutureWarning messages
warnings.simplefilter(action='ignore', category=FutureWarning)


PFIRS_COLUMNS = [
    'Acres_Approved', 'Acres_Burned', 'Acres_Requested', 'Active_On', 'Agency',
    'Air_Basin', 'Air_District', 'Approved_On', 'Aspect', 'Burn_Date', 'Burn_ID',
    'Burn_Status', 'Burn_Type', 'Burn_Unit', 'Completed',
    'County', 'Cover_Type', 'Crossroads', 'Fuel_Type', 'LMA_Name', 'LMA_Unit',
    'Landowner', 'Last_Updated', 'Latitude', 'Legal_Location', 'Longitude',
    'Max_Elevation', 'Min_Elevation', 'Monitor_Name',
    'Monitored', 'OBJECTID', 'Out_On', 'Patrol_On', 'Requested_On', 'SMP', 'Slope',
    'Tons_per_Acre', 'Total_Tons', 'Unit_Acres', 'Unit_Description', 'geometry'
]


def enrich_PFIRS(pfirs_gdb_path,
                 pfirs_layer_name,
                 treat_poly_gdf,
                 a_reference_gdb_path,
                 lookup_table_path,
                 start_year,
                 end_year,
                 output_gdb_path,
                 output_layer_name):

    logger.info("Load the PFIRS data into a GeoDataFrame")
    start = time.time()
    pfirs = gpd.read_file(pfirs_gdb_path, driver="OpenFileGDB", sql_dialect="OGRSQL", sql=f"SELECT *, OBJECTID FROM {pfirs_layer_name}")
    pfirs_lookup_df = pd.read_excel(lookup_table_path)
    logger.info(f"   time for loading {pfirs_layer_name}: {time.time()-start}")
    
    # construct dictionary from lookup table for agency name remaping and anonymous timber company parsing
    pfirs_lookup_dict = pd.Series(pfirs_lookup_df.agency_calc_field_2.values,index=pfirs_lookup_df.Agency).to_dict()
    anonymous_org_list = pfirs_lookup_df[pfirs_lookup_df["Org Type"] == "Industrial Timber"].Agency.tolist()
    
    # Validate the input data
    verify_gdf_columns(pfirs, PFIRS_COLUMNS, logger)

    # Convert to EPSG 3310
    pfirs = pfirs.to_crs(3310)
    show_columns(logger, pfirs, "pfirs")

    logger.info("Performing Standardization")
    logger.info("   step 1/8 remove some agencies")
    excluded_agencies = [
        'Cal Fire', 'US Forest Service', 'US Fish and Wildlife Services', 
        'Bureau of Land Management', 'National Park Service'
    ]
    gdf = pfirs[~pfirs['Agency'].isin(excluded_agencies)].copy()
    
    logger.info("   step 2/8 rename fields")
    # Rename fields
    gdf = gdf.rename(columns={
        'Agency': 'AGENCY_',
        'County': 'COUNTY_'
    })

    logger.info("   step 3/8 adding common columns...")
    gdf = add_common_columns(gdf)
    
    logger.info("   step 4/8 import attributes")
    # Calculate new fields
    gdf['PROJECTID_USER'] = 'PFIRS-' + gdf.index.astype(str)

    # remap agency name 

    # not sure how AGENCY is None at end result but ORG_ADMIN_p, which is copy of AGENCY,
    # is populated
    gdf['AGENCY'] = gdf['AGENCY_'].apply(lambda x: pfirs_lookup_dict.get(x, 'OTHER'))
    gdf['ORG_ADMIN_p'] = gdf['AGENCY']
    gdf['PROJECT_CONTACT'] = 'Jason Branz'
    gdf['PROJECT_EMAIL'] = 'jason.branz@arb.ca.gov'
    gdf['ADMINISTERING_ORG'] = gdf['AGENCY']
    gdf['PROJECT_NAME'] = gdf['Burn_Unit']
    gdf['PRIMARY_FUNDING_SOURCE'] = 'LOCAL'
    gdf['PRIMARY_FUNDING_ORG'] = 'OTHER'
    # parsing anonymity for private timber companies
    gdf['IMPLEMENTING_ORG'] = gdf['AGENCY_'].apply(lambda x: 'Timber Companies' if x in anonymous_org_list else x)
    gdf['TRMTID_USER'] = 'PFIRS-' + gdf.index.astype(str)
    gdf['PROJECTNAME_'] = None
    gdf['ORG_ADMIN_t'] = None
    gdf['BVT_USERD'] = 'NO'
    gdf['ACTIVITY_END'] = gdf['Burn_Date']
    gdf['ACTIVITY_STATUS'] = 'COMPLETE'
    gdf['ACTIVITY_QUANTITY'] = gdf['Acres_Burned']
    gdf['ACTIVITY_UOM'] = 'AC'
    gdf['ADMIN_ORG_NAME'] = 'CARB'
    gdf['IMPLEM_ORG_NAME'] = gdf['AGENCY_']
    gdf['PRIMARY_FUND_SRC_NAME'] = 'LOCAL'
    gdf['PRIMARY_FUND_ORG_NAME'] = 'OTHER'
    gdf['Source'] = 'PIFIRS'



    
    def map_burn_type(burn_type):
        mapping = {
            'Broadcast': 'Broadcast Burn',
            'Unknown': 'Broadcast Burn',
            'Hand Pile': 'Hand Pile Burn',
            'Pile' : 'Hand Pile Burn',
            'Mixed Piles': 'Hand Pile Burn',
            'Machine Pile': 'Machine Pile Burn',
            'Landing Pile': 'Landing Pile Burn',
            'Multiple Fuels': 'Broadcast Burn',
            'UNK': 'Broadcast Burn'
        }
        return mapping.get(burn_type, burn_type)
    gdf['Crosswalk'] = gdf['Burn_Type'].apply(map_burn_type)

    # remove all broadcast burn data for all industry groups except FWS Forestry LLC 
    ### 1. everything from non-timber company and FWS Forestry LLC
    ### 2. non-broadcast burn only from timber company
    mask_1 = (gdf['AGENCY_'] == 'FWS Forestry LLC') | (gdf['AGENCY'] != 'Timber Companies')
    mask_2 = ~mask_1 & (gdf['Burn_Type'] != 'Broadcast')

    gdf = gdf[mask_1 + mask_2]

    logger.info(f"      standardized has {len(gdf)} records")    
    logger.info("   step 5/8 remove points that intersect burn polygons")

    logger.info("   step 6/8 Remove Unnecessary Columns...")
    gdf = keep_fields(gdf)
    show_columns(logger, gdf, "standardized gdf")    

    logger.info("Performing Enrichments")
    enriched_gdf = enrich_points(gdf, a_reference_gdb_path, start_year, end_year)

    logger.info("   step 7/8 Assign Domains...")
    enriched_gdf = assign_domains(enriched_gdf)


    # check if clipping polygon layer is provided
    if treat_poly_gdf is not None:
        # Filter treatment polygons to only activity types that PFIRS duplicates
        rx_burns = treat_poly_gdf[treat_poly_gdf['ACTIVITY_DESCRIPTION'].isin(['BROADCAST_BURN', 'PILE_BURN'])]
        
        # Iterate through unique years to find intersecting duplicate treatment
        out_index = []
        for y in enriched_gdf.Year_txt.unique():
            # Subset to year
            treatment_subset = rx_burns[rx_burns.Year_txt == y]
            enriched_subset = enriched_gdf[enriched_gdf.Year_txt == y]
            # Spatial join to find intersecting points
            intersecting = gpd.sjoin(enriched_subset, treatment_subset, how='inner', predicate='intersects', lsuffix='poly', rsuffix='pfirs')
            out_index += list(set(enriched_subset.index) - set(intersecting.index))
        
        enriched_gdf = enriched_gdf.loc[out_index]

    # TEMP FIX: reassign AGENCY with ORG_ADMIN_p
    enriched_gdf['AGENCY'] = enriched_gdf['ORG_ADMIN_p'] 
    
    logger.info("   step 8/8 Save Result...")
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

    pfirs_input_gdb_path = config_inputs['pfirs']['input']['gdb_path']
    pfirs_input_layer_name = config_inputs['pfirs']['input']['layer_name']
    treat_poly_gdb_path = config_inputs['appended']['gdb_path']
    treat_poly_layer_name = config_inputs['appended']['polygon_layer_name']
    # if appended polygons layer is not finished, leave this to None
    treat_poly_gdf = gpd.read_file(treat_poly_gdb_path, driver="OpenFileGDB", layer=treat_poly_gdb_path)
    lookup_table_path = config_inputs['pfirs']['input']['excel_path']

    a_reference_gdb_path = config_inputs['global']['reference_gdb']
    start_year, end_year = config_inputs['global']['start_year'], config_inputs['global']['end_year']
    output_format_dict = {'start_year': start_year,
                          'end_year': end_year,
                          'date': datetime.today().strftime('%Y%m%d')}
    output_gdb_path = config_inputs['pfirs']['output']['gdb_path'].format(**output_format_dict)
    output_layer_name = config_inputs['pfirs']['output']['layer_name'].format(**output_format_dict)
    

    enrich_PFIRS(pfirs_input_gdb_path,
                    pfirs_input_layer_name,
                    treat_poly_gdf,
                    a_reference_gdb_path,
                    lookup_table_path,
                    start_year,
                    end_year,
                    output_gdb_path,
                    output_layer_name)

    # Get memory usage in bytes, convert to MB
    memory_usage = process.memory_info().rss / 1024 / 1024
    logger.info(f"Memory usage: {memory_usage:.2f} MB")
    

