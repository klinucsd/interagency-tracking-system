

"""
# Description: Converts the Timber Industry Nonspatial dataset into
#              the Task Force standardized schema.  Dataset is
#              enriched with vegetation, ownership, county, WUI, 
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
from shapely.geometry import Point

from its_logging.logger_config import logger
from utils.its_utils import clip_to_california, get_wfr_tf_template
from utils.gdf_utils import repair_geometries, show_columns, verify_gdf_columns
from utils.add_common_columns import add_common_columns
from utils.enrich_points import enrich_points
from utils.keep_fields import keep_fields
from utils.assign_domains import assign_domains
from utils.save_gdf_to_gdb import save_gdf_to_gdb


logger = logging.getLogger('enrich.Timber_NSpatial')

# Suppress pyogrio INFO logs
logging.getLogger("pyogrio").setLevel(logging.WARNING)

# Suppress specific warnings
warnings.filterwarnings("ignore", category=RuntimeWarning)

# Suppress specific FutureWarning messages
warnings.simplefilter(action='ignore', category=FutureWarning)

# Required columns for timber industry spatial data
TIMBER_NONSPATIAL_COLUMNS = [
    'ACTIVITY CATEGORY', 'ACTIVITY DESCRIPTION', 'ACTIVITY END', 'ACTIVITY QUANTITY',
    'ACTIVITY START', 'ACTIVITY STATUS', 'ACTIVITY UNITS', 'ADMINISTERING ORG',
    'BROAD VEGETATION TYPE', 'COUNTY', 'IN WUI', 'IS BVT USER DEFINED',
    'PRIMARY OWNERSHIP GROUP', 'TASK FORCE REGION'
]


def update_enriched_data(gdf):
    """
    Due to faked locations and missing crosswalk data, some vegetation types and the 
    columns "ACTIVITY_CAT," "PRIMARY_OBJECTIVE," and "COUNTS_TO_MAS" contain errors. 
    This function corrects these issues manually. Please note that this function 
    covers all cases in the element data; otherwise, you may need to modify the function.

    Otherwise modify the input Crosswalk table
    """
    # Activity mapping dictionary
    activity_mapping = {
        "AMW_AREA_RESTOR": ("WATSHD_IMPRV", "MTN_MEADOW_RESTOR"),
        "CHIPPING": ("MECH_HFR", "OTHER_FUELS_REDUCTION"),
        "COMM_THIN": ("TIMB_HARV", "OTHER_FUELS_REDUCTION"),
        "GRP_SELECTION_HARVEST": ("TIMB_HARV", "BIOMASS_UTIL"),
        "HERBICIDE_APP": ("MECH_HFR", "OTHER_FUELS_REDUCTION"),
        "LOP_AND_SCAT": ("MECH_HFR", "OTHER_FUELS_REDUCTION"),
        "MASTICATION": ("MECH_HFR", "OTHER_FUELS_REDUCTION"),
        "MOWING": ("MECH_HFR", "OTHER_FUELS_REDUCTION"),
        "OAK_WDLND_MGMT": ("WATSHD_IMPRV", "OTHER_FUELS_REDUCTION"),
        "PILE_BURN": ("BENEFICIAL_FIRE", "OTHER_FUELS_REDUCTION"),
        "PILING": ("MECH_HFR", "OTHER_FUELS_REDUCTION"),
        "REHAB_UNDRSTK_AREA": ("TIMB_HARV", "ECO_RESTOR"),
        "SINGLE_TREE_SELECTION": ("TIMB_HARV", "BIOMASS_UTIL"),
        "THIN_MAN": ("MECH_HFR", "OTHER_FUELS_REDUCTION"),
        "TRANSITION_HARVEST": ("TIMB_HARV", "BIOMASS_UTIL"),
        "TREE_PLNTING": ("TREE_PLNTING", "REFORESTATION"),
        "VARIABLE_RETEN_HARVEST": ("TIMB_HARV", "BIOMASS_UTIL")
    }

    # Update based on ACTIVITY_DESCRIPTION
    mask = gdf['ACTIVITY_DESCRIPTION'].isin(activity_mapping.keys())
    gdf.loc[mask, 'ACTIVITY_CAT'] = gdf.loc[mask, 'ACTIVITY_DESCRIPTION'].map(lambda x: activity_mapping[x][0])
    gdf.loc[mask, 'PRIMARY_OBJECTIVE'] = gdf.loc[mask, 'ACTIVITY_DESCRIPTION'].map(lambda x: activity_mapping[x][1])

    # Value mapping dictionary
    value_mapping = {
        "Roadway Clearance": ("ROAD_CLEAR", "MECH_HFR", "OTHER_FUELS_REDUCTION"),
        "Alternative Prescription": ("TRANSITION_HARVEST", "TIMB_HARV", "BIOMASS_UTIL"),
        "Sanitation Harvest": ("SANI_HARVEST", "TIMB_HARV", "BIOMASS_UTIL"),
        "Landing Treated": ("LANDING_TRT", "MECH_HFR", "OTHER_FUELS_REDUCTION"),
        "Invasive Plant Removal": ("INV_PLANT_REMOVAL", "MECH_HFR", "ECO_RESTOR")
    }

    # Update based on Crosswalk
    mask = gdf['Crosswalk'].isin(value_mapping.keys())
    gdf.loc[mask, 'ACTIVITY_DESCRIPTION'] = gdf.loc[mask, 'Crosswalk'].map(lambda x: value_mapping[x][0])
    gdf.loc[mask, 'ACTIVITY_CAT'] = gdf.loc[mask, 'Crosswalk'].map(lambda x: value_mapping[x][1])
    gdf.loc[mask, 'PRIMARY_OBJECTIVE'] = gdf.loc[mask, 'Crosswalk'].map(lambda x: value_mapping[x][2])

    # Update Crosswalk field
    gdf['Crosswalk'] = gdf['ACTIVITY_DESCRIPTION']

    # Update COUNTS_TO_MAS field
    gdf['COUNTS_TO_MAS'] = 'YES'

    gdf['ADMINISTERING_ORG'] = gdf['AGENCY']

    return gdf



def enrich_Timber_Nonspatial(tn_input_excel_path,
                             a_reference_gdb_path,
                             start_year,
                             end_year,
                             output_gdb_path,
                             output_layer_name):


    logger.info("Load the Timeber Industry Nonspatial data into a DataFrame")
    start = time.time()
    excel = pd.ExcelFile(tn_input_excel_path)
    logger.info(f"   time for loading {tn_input_excel_path}: {time.time()-start}")
    
    logger.info("Performing Standardization")
    logger.info("   step 1/10 convert Excel sheet to table")
    tn_df = pd.read_excel(excel, sheet_name=excel.sheet_names[0])
    
    # Validate the input data
    verify_gdf_columns(tn_df, TIMBER_NONSPATIAL_COLUMNS, logger)
    show_columns(logger, tn_df, "tn_df")

    # Rename fields with underscore suffix
    logger.info("   step 2/10 rename and add fields")    
    rename_fields = {
        'ACTIVITY CATEGORY': 'ACTIVITY_CATEGORY',
        'ACTIVITY STATUS': 'ACTIVITY_STATUS',
        'ACTIVITY UNITS': 'ACTIVITY_UNITS',
        'IS BVT USER DEFINED': 'IS_BVT_USER_DEFINED',
        'TASK FORCE REGION': 'TASK_FORCE_REGION',

        'ACTIVITY DESCRIPTION': 'ACTIVITY_DESCRIPTION_',
        'BROAD VEGETATION TYPE': 'BROAD_VEGETATION_TYPE_',
        'ACTIVITY STATUS': 'ACTIVITY_STATUS_',
        'ACTIVITY QUANTITY': 'ACTIVITY_QUANTITY_',
        'ACTIVITY START': 'ACTIVITY_START_',
        'ACTIVITY END': 'ACTIVITY_END_',
        'ADMINISTERING ORG': 'ADMINISTERING_ORG_',
        'COUNTY': 'COUNTY_',
        'IN WUI': 'IN_WUI_',
        'PRIMARY OWNERSHIP GROUP': 'PRIMARY_OWNERSHIP_GROUP_'        
    }
    tn_df = tn_df.rename(columns=rename_fields)
    show_columns(logger, tn_df, "tn_df")

    logger.info("   step 3/10 adding common columns...")
    tn_df = add_common_columns(tn_df)
    
    logger.info("   step 4/10 calculate fields")
    # Copy values to standard fields
    field_mappings = {
        'ACTIVITY_DESCRIPTION': 'ACTIVITY_DESCRIPTION_',
        'ACTIVITY_CAT': 'ACTIVITY_CATEGORY',
        'BROAD_VEGETATION_TYPE': 'BROAD_VEGETATION_TYPE_',
        'BVT_USERD': 'IS_BVT_USER_DEFINED',
        'ACTIVITY_STATUS': 'ACTIVITY_STATUS_',
        'ACTIVITY_QUANTITY': 'ACTIVITY_QUANTITY_',
        'ACTIVITY_UOM': 'ACTIVITY_UNITS',
        'ACTIVITY_START': 'ACTIVITY_START_',
        'ACTIVITY_END': 'ACTIVITY_END_',
        'ADMIN_ORG_NAME': 'ADMINISTERING_ORG_',
        'COUNTY': 'COUNTY_',
        'IN_WUI': 'IN_WUI_',
        'REGION': 'TASK_FORCE_REGION',
        'PRIMARY_OWNERSHIP_GROUP': 'PRIMARY_OWNERSHIP_GROUP_'
    }
    
    for new_field, old_field in field_mappings.items():
        tn_df[new_field] = tn_df[old_field]

    tn_df['ACTIVITY_DESCRIPTION'] = tn_df['ACTIVITY_DESCRIPTION'].str.strip()    
    # print(tn_df[['ACTIVITY_DESCRIPTION', 'ACTIVITY_CATEGORY']])
        
    # Set constant values
    tn_df['PRIMARY_FUNDING_SOURCE'] = 'PRIVATE'
    tn_df['PRIMARY_FUNDING_ORG'] = 'PRIVATE_INDUSTRY'
    tn_df['AGENCY'] = tn_df['ADMIN_ORG_NAME']
    tn_df['ADMINISTERING_ORG'] = tn_df['ADMIN_ORG_NAME']
    tn_df['IMPLEMENTING_ORG'] = tn_df['ADMIN_ORG_NAME']
    tn_df['ORG_ADMIN_p'] = tn_df['ADMIN_ORG_NAME']
    tn_df['ORG_ADMIN_t'] = tn_df['ADMIN_ORG_NAME']
    tn_df['ORG_ADMIN_a'] = tn_df['ADMIN_ORG_NAME']
    tn_df['PRIMARY_FUND_SRC_NAME'] = tn_df['PRIMARY_FUNDING_SOURCE']
    tn_df['PRIMARY_FUND_ORG_NAME'] = tn_df['PRIMARY_FUNDING_ORG']
    tn_df['Source'] = 'Industrial Timber'
    tn_df['TRMT_GEOM'] = 'POINT'
    
    # Clean up activity descriptions
    activity_mapping = {
        'Fuel Break (pursuant to FPRs)': 'Thinning (Manual)',
        'Group Selection': 'Group Selection Harvest',
        'Group Selection ': 'Group Selection Harvest',
        'Rehabilitation of Understocked Area ': 'Rehabilitation of Understocked Area'
    }
    tn_df['ACTIVITY_DESCRIPTION'] = tn_df['ACTIVITY_DESCRIPTION'].replace(activity_mapping)
    tn_df['Crosswalk'] = tn_df['ACTIVITY_DESCRIPTION']
    
    # Add coordinates based on activity description
    lat_mapping = {
        'Aspen/Meadow/Wet Area Restoration': 37.482646,
        'Chipping': 37.627402,
        'Commercial Thin': 37.465931,
        'Group Selection Harvest': 37.464555,
        'Herbicide Application': 37.655861,
        'Lop and Scatter': 37.551419,
        'Mastication': 37.462986,
        'Mowing': 37.4928,
        'Oak Woodland Management': 37.534775,
        'Pile Burning': 37.572375,
        'Piling': 37.461292,
        'Rehabilitation of Understocked Area': 37.525904,
        'Single Tree Selection': 37.523401,
        'Thinning (Manual)': 37.625516,
        'Transition Harvest': 37.506553,
        'Tree Planting': 37.534028,
        'Variable Retention Harvest': 37.520369,

        'Alternative Prescription': 37.488371,
        'Fuel Break (pursuant to FPRs)': 37.473531,
        'Group Selection': 37.465912,
        'Invasive Plant Removal': 37.614452,
        'Landing Treated': 37.605668,
        'Roadway Clearance': 37.507632,
        'Sanitation Harvest': 37.591843,
        'Site Preparation': 37.562715,
        'Thinning (Mechanical)': 37.551381,
    }
    
    lon_mapping = {
        'Aspen/Meadow/Wet Area Restoration': -123.21911033,
        'Chipping': -123.249295,
        'Commercial Thin': -123.390699,
        'Group Selection Harvest': -123.438758,
        'Herbicide Application': -123.47236,
        'Lop and Scatter': -123.222054,
        'Mastication': -123.49278,
        'Mowing': -123.219545,
        'Oak Woodland Management': -123.221341,
        'Pile Burning': -123.222952,
        'Piling': -123.550139,
        'Rehabilitation of Understocked Area': -123.459845,
        'Single Tree Selection': -123.545137,
        'Thinning (Manual)': -123.317925,
        'Transition Harvest': -123.220133,
        'Tree Planting': -123.322739,
        'Variable Retention Harvest': -123.220724,

        'Alternative Prescription': -123.339323,
        'Fuel Break (pursuant to FPRs)': -123.349238,
        'Group Selection':  -123.358234,
        'Invasive Plant Removal': -123.483481,
        'Landing Treated': -123.398283,
        'Roadway Clearance': -123.383823,
        'Sanitation Harvest': -123.373429,
        'Site Preparation': -123.323224,
        'Thinning (Mechanical)': -123.373398,
        
    }

    # Revised grid geometery
    lat_min, lat_max = min(lat_mapping.values()), max(lat_mapping.values())
    lon_min, lon_max = min(lon_mapping.values()), max(lon_mapping.values())

    delta = int(np.ceil(np.sqrt(len(tn_df))))
    lat_delta = (lat_max - lat_min)/delta
    lon_delta = (lon_max - lon_min)/delta

    coords = np.mgrid[lon_min:lon_max:lon_delta, lat_min:lat_max:lat_delta].T
    coords = coords.reshape(coords.shape[0]*coords.shape[1], -1)

    tn_df = tn_df.sort_values(by='ACTIVITY_DESCRIPTION')

    tn_df.geometry = [Point(coords[i]) for i in range(len(tn_df))]

    #tn_df['LATITUDE'] = tn_df['ACTIVITY_DESCRIPTION'].map(lat_mapping)
    #tn_df['LONGITUDE'] = tn_df['ACTIVITY_DESCRIPTION'].map(lon_mapping)



    # Convert to GeoDataFrame
    logger.info(f"   step 5/10 converting Table to Geodataframe")
    geometry = [Point(xy) for xy in zip(tn_df['LONGITUDE'], tn_df['LATITUDE'])]
    gdf = gpd.GeoDataFrame(tn_df, geometry=geometry, crs='EPSG:4326')

    # Project to California Albers (EPSG:3310)
    gdf = gdf.to_crs('EPSG:3310')
    
    # Define essential dissolve fields
    essential_fields = [
        'ACTIVITY_DESCRIPTION', 
        'ACTIVITY_STATUS',
        'COUNTY',
        'BROAD_VEGETATION_TYPE',
        'ACTIVITY_START',
        'ACTIVITY_END',
        'ADMIN_ORG_NAME'
    ]

    # Fill NaN values
    gdf = gdf.fillna({col: '' for col in essential_fields})

    # Create aggregation dict for all columns
    agg_dict = {col: 'first' for col in gdf.columns if col not in ['geometry', 'ACTIVITY_QUANTITY'] and col not in essential_fields}
    agg_dict['ACTIVITY_QUANTITY'] = 'sum'

    # Dissolve with all columns
    gdf_dissolved = gdf.dissolve(by=essential_fields, aggfunc=agg_dict).reset_index()
    
    # Generate IDs
    gdf_dissolved['PROJECTID_USER'] = 'TI-' + gdf_dissolved.index.astype(str)
    gdf_dissolved['PROJECT_NAME'] = gdf_dissolved['PROJECTID_USER']
    gdf_dissolved['TRMTID_USER'] = gdf_dissolved['PROJECTID_USER']
    gdf_dissolved['PROJECTNAME_'] = None
    
    # Remove unnecessary columns
    logger.info("   step 6/10 Remove Unnecessary Columns...")
    gdf_dissolved = keep_fields(gdf_dissolved)
    show_columns(logger, gdf_dissolved, "gdf_dissolved")


    
    # Enrich points
    logger.info(f"   step 7/10 Enrich Points")
    tn_enriched = enrich_points(gdf_dissolved,  a_reference_gdb_path, start_year, end_year)

    logger.info(f"   step 8/10 Fix board veg types and others")
    tn_enriched = update_enriched_data(tn_enriched)
    
    logger.info(f"   step 9/10 Assign Domains...")
    tn_enriched = assign_domains(tn_enriched)

    
    logger.info(f"   step 10/10 Save Result...")
    save_gdf_to_gdb(tn_enriched,
                    output_gdb_path,
                    output_layer_name,
                    group_name="c_Enriched")

    
if __name__ == "__main__":
    # Get the current process ID
    process = psutil.Process(os.getpid())

    # load config file path yaml
    with open("..\config.yaml", 'r') as stream:
        config_inputs = yaml.safe_load(stream)

    tn_input_excel_path = config_inputs['sources']['timber_industry_nonspatial']['input']['excel_path']
    a_reference_gdb_path = config_inputs['global']['reference_gdb']
    start_year, end_year = config_inputs['global']['start_year'], config_inputs['global']['end_year']
    output_format_dict = {'start_year': start_year,
                          'end_year': end_year,
                          'date': datetime.today().strftime('%Y%m%d')}
    output_gdb_path = config_inputs['sources']['timber_industry_nonspatial']['output']['gdb_path'].format(**output_format_dict)
    output_layer_name = config_inputs['sources']['timber_industry_nonspatial']['output']['layer_name'].format(**output_format_dict)
    
    enrich_Timber_Nonspatial(tn_input_excel_path,
                             a_reference_gdb_path,
                             start_year,
                             end_year,
                             output_gdb_path,
                             output_layer_name)

    # Get memory usage in bytes, convert to MB
    memory_usage = process.memory_info().rss / 1024 / 1024
    logger.info(f"Memory usage: {memory_usage:.2f} MB")
