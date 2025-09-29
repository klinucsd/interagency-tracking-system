

"""
# Description: Converts the California Department of Natural 
#              Resources's fuels treatments dataset 
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
v
import numpy as np
import pandas as pd
import geopandas as gpd
from datetime import datetime

from its_logging.logger_config import logger
from utils.its_utils import clip_to_california, get_wfr_tf_template
from utils.gdf_utils import repair_geometries, show_columns, verify_gdf_columns, capitalize_columns
from utils.add_common_columns import add_common_columns
from utils.enrich_polygons import enrich_polygons
from utils.enrich_points import enrich_points
from utils.enrich_lines import enrich_lines
from utils.keep_fields import keep_fields
from utils.assign_domains import assign_domains
from utils.save_gdf_to_gdb import save_gdf_to_gdb


logger = logging.getLogger('enrich.enrich_CNRA')

# Suppress pyogrio INFO logs
logging.getLogger("pyogrio").setLevel(logging.WARNING)

# Suppress specific warnings
warnings.filterwarnings("ignore", category=RuntimeWarning)

# Suppress specific FutureWarning messages
warnings.simplefilter(action='ignore', category=FutureWarning)


CNRA_POLYGON_COLUMNS = [
    'county', 'created_date',
    'created_user', 'geometry', 'globalid', 'globalid_text', 'in_wui',
    'last_edited_date', 'last_edited_user', 'org_admin_t', 'primary_objective',
    'primary_ownership_group', 'projectid', 'projectid_user', 'projectname_',
    'region', 'retreatment_date_est', 'secondary_objective', 'tertiary_objective',
    'treatment_area', 'treatment_end', 'treatment_name', 'treatment_start',
    'treatment_status', 'trmtid_user', 'validationstatus']

CNRA_LINE_COLUMNS = [
    'county', 'created_date', 'created_user',
    'geometry', 'globalid', 'globalid_text', 'in_wui', 'last_edited_date',
    'last_edited_user', 'org_admin_t', 'primary_objective', 'primary_ownership_group',
    'projectid', 'projectid_user', 'projectname_', 'region', 'retreatment_date_est',
    'secondary_objective', 'tertiary_objective', 'treatment_area', 'treatment_end',
    'treatment_name', 'treatment_start', 'treatment_status', 'trmtid_user', 'validationstatus']

CNRA_POINT_COLUMNS = [
    'county', 'created_date', 'created_user', 'geometry', 'globalid',
    'globalid_text', 'in_wui', 'last_edited_date', 'last_edited_user', 'org_admin_t',
    'primary_objective', 'primary_ownership_group', 'projectid', 'projectid_user',
    'projectname_', 'region', 'retreatment_date_est', 'secondary_objective',
    'tertiary_objective', 'treatment_area', 'treatment_end', 'treatment_name',
    'treatment_start', 'treatment_status', 'trmtid_user', 'validationstatus']

CNRA_PROJECT_COLUMNS = [
    'administering_org', 'agency',
    'created_date', 'created_user', 'geometry', 'globalid', 'globalid_text',
    'implementing_org', 'last_edited_date', 'last_edited_user', 'latitude',
    'longitude', 'org_admin_p', 'primary_funding_org', 'primary_funding_source',
    'project_contact', 'project_email', 'project_end', 'project_name',
    'project_start', 'project_status', 'projectid_user', 'validationstatus']

CNRA_ACTIVITY_COLUMNS = [
    'activid_user', 'activity_cat', 'activity_description',
    'activity_end', 'activity_name', 'activity_prct', 'activity_quantity',
    'activity_start', 'activity_status', 'activity_uom', 'admin_org_name',
    'broad_vegetation_type', 'bvt_userd', 'counts_to_mas', 'created_date',
    'created_user', 'globalid', 'implem_org_name', 'last_edited_date',
    'last_edited_user', 'org_admin_a', 'primary_fund_org_name',
    'primary_fund_src_name', 'primary_objective', 'residue_fate',
    'residue_fate_quantity', 'residue_fate_units', 'secondary_fund_org_name',
    'secondary_fund_src_name', 'tertiary_fund_org_name', 'tertiary_fund_src_name',
    'treatmentid_ln', 'treatmentid_poly', 'treatmentid_pt', 'trmt_geom',
    'trmtid_user']


def create_standardized_features(template_gdf):
    """
    Create a new GeoDataFrame with standardized schema based on template
    """
    # Create empty GeoDataFrame with same schema as template
    standardized = gpd.GeoDataFrame(
        columns=template_gdf.columns,
        geometry='geometry',
        crs=template_gdf.crs
    )
    return standardized


def prepare_activity_table(cnra_activities):
    """
    Prepare and standardize the activity table for both points and polygons
    """

    logger.info("   Part 2 Prepare Activity Table")
    # Make a copy of activity table
    activity_table = cnra_activities.copy()
    
    # Define ifelse functions for various field calculations
    def ifelse_treatment_id(row):

        if row['TREATMENTID_LN'] is not None:
            return row['TREATMENTID_LN']
        if row['TREATMENTID_PT'] is not None:
            return row['TREATMENTID_PT']
        return row['TREATMENTID_POLY']
    
    def ifelse_trmtid_user(id_val, alt_val):
        return alt_val if id_val is None else id_val
    
    # Apply the calculations
    activity_table['TREATMENTID_'] = activity_table.apply(ifelse_treatment_id, axis=1)

    # UPDATE
    # drop na in unique key
    activity_table = activity_table.dropna(subset=['TREATMENTID_'])

    
    logger.info("      step 2/17 remove milliseconds from dates")
    # Convert and clean date fields
    activity_table['ACTIVITY_END'] = pd.to_datetime(activity_table['ACTIVITY_END']).dt.date
    activity_table['ACTIVITY_START'] = pd.to_datetime(activity_table['ACTIVITY_START']).dt.date
    
    logger.info("      step 3/17 create standardized activity table")
    # Create standardized schema
    standardized_schema = {
        'ACTIVID_USER': 'str',
        'TREATMENTID_': 'str',
        'ORG_ADMIN_a': 'str',
        'ACTIVITY_DESCRIPTION': 'str',
        'ACTIVITY_CAT': 'str',
        'BROAD_VEGETATION_TYPE': 'str',
        'BVT_USERD': 'str',
        'ACTIVITY_STATUS': 'str',
        'ACTIVITY_QUANTITY': 'float64',
        'ACTIVITY_UOM': 'str',
        'ACTIVITY_START': 'datetime64[ns]',
        'ACTIVITY_END': 'datetime64[ns]',
        'ADMIN_ORG_NAME': 'str',
        'IMPLEM_ORG_NAME': 'str',
        'PRIMARY_FUND_SRC_NAME': 'str',
        'PRIMARY_FUND_ORG_NAME': 'str',
        'SECONDARY_FUND_SRC_NAME': 'str',
        'SECONDARY_FUND_ORG_NAME': 'str',
        'TERTIARY_FUND_SRC_NAME': 'str',
        'TERTIARY_FUND_ORG_NAME': 'str',
        'ACTIVITY_PRCT': 'Int16',
        'RESIDUE_FATE': 'str',
        'RESIDUE_FATE_QUANTITY': 'float64',
        'RESIDUE_FATE_UNITS': 'str',
        'ACTIVITY_NAME': 'str',
        'VAL_STATUS_a': 'str',
        'VAL_MSG_a': 'str',
        'VAL_RUNDATE_a': 'datetime64[ns]',
        'REVIEW_STATUS_a': 'str',
        'REVIEW_MSG_a': 'str',
        'REVIEW_RUNDATE_a': 'datetime64[ns]',
        'DATALOAD_STATUS_a': 'str',
        'DATALOAD_MSG_a': 'str',
        'Source': 'str',
        'Year': 'Int64',
        'Year_txt': 'str',
        'Act_Code': 'Int64',
        'Crosswalk': 'str',
        'Federal_FY': 'Int64',
        'State_FY': 'Int64',
        'TRMTID_USER': 'str',
        'BATCHID_a': 'str',
        'TRMT_GEOM': 'str',
        'COUNTS_TO_MAS': 'str'
    }
        
    # Convert activity table to standardized schema
    standardized_df = pd.DataFrame(columns=standardized_schema.keys()).astype(standardized_schema)
    show_columns(logger, standardized_df, "standardized_df")

    logger.info("      step 4/17 import activities into standardized table")
    # Append activity data to standardized table
    # Only include columns that exist in the schema
    common_columns = list(set(activity_table.columns) & set(standardized_schema.keys()))
    activity_table = pd.concat([standardized_df, activity_table[common_columns]], ignore_index=True)
    show_columns(logger, activity_table, "activity_table")
    

    return activity_table


def prepare_project_table(cnra_projects):
    """
    Prepare and standardize the project table
    """

    logger.info("   Part 4 Prepare Project Table")
    # Process project table
    project_table = cnra_projects.copy()
    
    # Update agency field
    project_table['AGENCY'] = project_table['AGENCY'].apply(
        lambda x: 'CNRA' if x == 'CALFIRE' else x)
    
    # Update project ID
    logger.info("      step 7/17 calculate unique Project ID if null")
    project_table['PROJECTID_USER'] = project_table['GlobalID']
    project_table.loc[project_table['PROJECTID_USER'].notna(), 'PROJECTID_USER'] = \
        project_table.loc[project_table['PROJECTID_USER'].notna(), 'PROJECTID_USER'] + '-CNRA'
    
    # Remove duplicates
    project_table = project_table.drop_duplicates(subset=['PROJECTID_USER'], keep='first')
    
    return project_table


def enrich_standardized_features(standardized_features):
    """
    Standardize and enrich the standardized feature data
    """
    logger.info("   Part 6 Standardize and Enrich")
    logger.info("      step 11/17 calculate crosswalk")
    standardized_features['Crosswalk'] = standardized_features['ACTIVITY_DESCRIPTION']

    logger.info("      step 12/17 calculate source")
    standardized_features['Source'] = 'CNRA'

    logger.info("      step 13/17 calculate admin")

    # Update admin fields
    standardized_features['ORG_ADMIN_a'] = standardized_features['ORG_ADMIN_a'].fillna(standardized_features['ORG_ADMIN_t'])
    standardized_features['ORG_ADMIN_p'] = standardized_features['ORG_ADMIN_p'].fillna(standardized_features['ORG_ADMIN_t'])
    standardized_features['ADMINISTERING_ORG'] = standardized_features['ADMINISTERING_ORG'].fillna(standardized_features['ORG_ADMIN_t'])
    standardized_features['AGENCY'] = standardized_features['AGENCY'].fillna('CNRA')

    logger.info("      step 14/17 update status")
    standardized_features['ACTIVITY_STATUS'] = standardized_features['ACTIVITY_STATUS'].fillna('COMPLETE')

    logger.info("      step 15/17 update activity end date")

    # Update activity end dates
    def update_activity_end(row):
        if pd.isna(row['ACTIVITY_END']):
            if row['ACTIVITY_STATUS'] in ['ACTIVE', 'Active', 'COMPLETE', 'Complete']:
                return datetime.now()
            elif row['ACTIVITY_STATUS'] in ['PLANNED', 'Planned']:
                if pd.isna(row['ACTIVITY_START']):
                    return datetime.now()
                return row['ACTIVITY_START']
        return row['ACTIVITY_END']

    standardized_features['ACTIVITY_END'] = standardized_features.apply(update_activity_end, axis=1)

    standardized_features = add_common_columns(standardized_features)
    show_columns(logger, standardized_features, "standardized_features")    

    return standardized_features


def enrich_CNRA_features(
        input_features,
        cnra_activities,
        cnra_projects,
        a_reference_gdb_path,
        start_year,
        end_year,
        output_gdb_path,
        output_layer_name,
        feature_type):
    """
    Generic function to enrich both points and polygons
    
    Args:
        feature_type: Either 'point' or 'line' or 'polygon'
    """
    logger.info(f"Enrich the CNRA {feature_type}s...")
    
    logger.info("   Part 1 Prepare Features")
    input_features = input_features.copy()

    # UPDATE
    # drop nan val in global id
    # use globalid
    input_features = input_features.dropna(subset=['GlobalID'])

    # not used in update
    #input_features['PROJECTID_USER'] = input_features['PROJECTID_USER'].astype(str).str[:45] + '-CNRA'
    #input_features['TRMTID_USER'] = input_features['TRMTID_USER'].astype(str).str[:45] + '-CNRA'
    
    # Part 2 Prepare Activity Table
    activity_table = prepare_activity_table(cnra_activities)
    
    
    logger.info("   Part 3 - Combine CNRA Features and Activity Table")
    logger.info("      step 6/17 join polygon table and activity table")
    # Merge features with activity table
    merged_data = input_features.merge(
        activity_table,
        left_on='GlobalID',
        right_on='TREATMENTID_',
        how='left'
    )

    # UPDATE: update TRMTID_USER as TREATMENTID_ for reports
    logger.info("         calculate unique Treatment ID with postfix '-CNRA'")
    merged_data['TRMTID_USER'] = merged_data['GlobalID'] + '-CNRA'
        
    merged_data = merged_data.drop_duplicates()
    
    # Part 4 Prepare Project Table
    project_table = prepare_project_table(cnra_projects)
        
    logger.info("   Part 5 Join Project Table to Features/Activities")
    # Keep only the geometry from merged_data during merge
    # First, store the geometry for later use
    merged_geom = merged_data.geometry
    
    # Drop geometry columns before merge to prevent duplicates
    merged_data_no_geom = merged_data.drop(columns=['geometry'])
    project_table_no_geom = project_table.drop(columns=['geometry'])

    # UDPATE
    # merge condition: PROJECT_USER has human error per CNRA admin, change to GlobalID instead
    cnra_flat = merged_data.merge(
        project_table_no_geom,
        left_on='PROJECTID',
        right_on='GlobalID',
        how='left'
    )


    # UDPATE: update PROJECTID_USER as PROJECTID for reports
    cnra_flat['PROJECTID_USER'] = cnra_flat['PROJECTID']
    cnra_flat.loc[cnra_flat['PROJECTID_USER'].notna(), 'PROJECTID_USER'] = \
        cnra_flat.loc[cnra_flat['PROJECTID_USER'].notna(), 'PROJECTID_USER'] + '-CNRA'
    

    print("!!!!")
    print (len(cnra_flat['TRMTID_USER'].unique()))
    print (len(cnra_flat['PROJECTID_USER'].unique()))
    

    """
    # Perform the merge without geometries
    cnra_flat = merged_data_no_geom.merge(
        project_table_no_geom,
        on='PROJECTID_USER',
        how='left'
    )

    # Create GeoDataFrame with the original geometry
    cnra_flat = gpd.GeoDataFrame(
        cnra_flat, 
        geometry=merged_geom,
        crs=merged_data.crs
    )
    """
    
    logger.info("      step 8/17 copy features")
    # Create a deep copy to simulate CopyFeatures
    cnra_flat_copy = cnra_flat.copy(deep=True)
    
    logger.info("      step 9/17 create Features")
    # Create standardized feature class using template
    standardized_features = create_standardized_features(get_wfr_tf_template(a_reference_gdb_path))
    
    logger.info("      step 10/17 append")
    # First ensure the schema matches
    common_columns = list(set(standardized_features.columns) & set(cnra_flat_copy.columns))
    if 'geometry' not in common_columns:
        common_columns.append('geometry')
    
    # Create new GeoDataFrame with matching schema
    standardized_features = gpd.GeoDataFrame(
        cnra_flat_copy[common_columns],
        geometry='geometry',
        crs=standardized_features.crs
    )
    
    logger.info(f"      standardized has {len(standardized_features)} records")

    # Part 6 Standardize and Enrich
    standardized_features = enrich_standardized_features(standardized_features)
    
    # Convert to points if needed
    if feature_type == 'point' and not standardized_features.geom_type.eq('Point').all():
        standardized_features['geometry'] = standardized_features.geometry.centroid

    # MRCA reported activity should belong to SMMC
    standardized_features.loc[standardized_features.ADMINISTERING_ORG == 'MRCA', 'ADMINISTERING_ORG'] = 'SMMC'
        
    logger.info("   Part 7 Calculate Board Vegetation Types, Ownership and Others ... ")    
    standardized_features = keep_fields(standardized_features)
    
    if feature_type == 'polygon':
        standardized_features = enrich_polygons(standardized_features, a_reference_gdb_path, start_year, end_year)  
    elif feature_type == 'line':
        standardized_features = enrich_lines(standardized_features, a_reference_gdb_path, start_year, end_year)
    elif feature_type == 'point':
        standardized_features = enrich_points(standardized_features, a_reference_gdb_path, start_year, end_year)

    standardized_features = keep_fields(standardized_features)
        
    logger.info("   Part 8 Assign Domains...")
    standardized_features = assign_domains(standardized_features)
    show_columns(logger, standardized_features, "standardized_features")
    
    logger.info("   Part 9 Save Result...")
    save_gdf_to_gdb(standardized_features,
                    output_gdb_path,
                    f"{output_layer_name}_{feature_type}",
                    group_name="c_Enriched")
    

def enrich_CNRA(cnra_gdb_path,
                cnra_polygon_layer_name,
                cnra_line_layer_name,
                cnra_point_layer_name,
                cnra_project_polygon_layer_name,
                cnra_activity_layer_name,
                a_reference_gdb_path,
                start_year,
                end_year,
                output_gdb_path,
                output_layer_name):

    logger.info("Load the CNRA polygon layer into a GeoDataFrame")
    cnra_polygons = gpd.read_file(cnra_gdb_path, driver="OpenFileGDB", sql_dialect="OGRSQL", sql=f"SELECT * FROM {cnra_polygon_layer_name}")
    verify_gdf_columns(cnra_polygons, CNRA_POLYGON_COLUMNS, logger)
    cnra_polygons = capitalize_columns(cnra_polygons.to_crs(3310))
    show_columns(logger, cnra_polygons, "cnra_polygons")

    logger.info("Load the CNRA line layer into a GeoDataFrame")
    cnra_lines = gpd.read_file(cnra_gdb_path, driver="OpenFileGDB", sql_dialect="OGRSQL", sql=f"SELECT * FROM {cnra_line_layer_name}")
    verify_gdf_columns(cnra_lines, CNRA_LINE_COLUMNS, logger)
    cnra_lines = capitalize_columns(cnra_lines.to_crs(3310))
    show_columns(logger, cnra_lines, "cnra_lines")
    
    logger.info("Load the CNRA point layer into a GeoDataFrame")
    cnra_points = gpd.read_file(cnra_gdb_path, driver="OpenFileGDB", sql_dialect="OGRSQL", sql=f"SELECT * FROM {cnra_point_layer_name}")
    verify_gdf_columns(cnra_points, CNRA_POINT_COLUMNS, logger)
    cnra_points = capitalize_columns(cnra_points.to_crs(3310))
    show_columns(logger, cnra_points, "cnra_points")

    logger.info("Load the CNRA project polygon layer into a GeoDataFrame")
    cnra_projects = gpd.read_file(cnra_gdb_path, driver="OpenFileGDB", sql_dialect="OGRSQL", sql=f"SELECT * FROM {cnra_project_polygon_layer_name}")
    verify_gdf_columns(cnra_projects, CNRA_PROJECT_COLUMNS, logger)    
    cnra_projects = capitalize_columns(cnra_projects.to_crs(3310))
    show_columns(logger, cnra_projects, "cnra_projects")

    logger.info("Load the CNRA activity layer into a DataFrame")
    cnra_activities = gpd.read_file(cnra_gdb_path, driver="OpenFileGDB", sql_dialect="OGRSQL", sql=f"SELECT *  FROM {cnra_activity_layer_name}")
    verify_gdf_columns(cnra_activities, CNRA_ACTIVITY_COLUMNS, logger)
    cnra_activities = capitalize_columns(cnra_activities)
    show_columns(logger, cnra_activities, "cnra_activities")

    # enrich polygons
    enrich_CNRA_features(
        cnra_polygons, 
        cnra_activities, 
        cnra_projects, 
        a_reference_gdb_path,
        start_year,
        end_year,
        output_gdb_path,
        output_layer_name,
        'polygon'
    )

    # enrich lines
    enrich_CNRA_features(
        cnra_lines, 
        cnra_activities, 
        cnra_projects, 
        a_reference_gdb_path,
        start_year,
        end_year,
        output_gdb_path,
        output_layer_name,
        'line'
    )
    
    # enrich points
    enrich_CNRA_features(
        cnra_points, 
        cnra_activities, 
        cnra_projects, 
        a_reference_gdb_path,
        start_year,
        end_year,
        output_gdb_path,
        output_layer_name,
        'point'
    )
    
if __name__ == "__main__":
    # Get the current process ID
    process = psutil.Process(os.getpid())

    # load config file path yaml
    with open("..\config.yaml", 'r') as stream:
        config_inputs = yaml.safe_load(stream)

    cnra_input_gdb_path = config_inputs['cnra']['input']['gdb_path']
    cnra_polygon_layer_name = config_inputs['cnra']['input']['polygon_layer_name']
    cnra_line_layer_name = config_inputs['cnra']['input']['line_layer_name']
    cnra_point_layer_name = config_inputs['cnra']['input']['point_layer_name']
    cnra_project_polygon_layer_name = config_inputs['cnra']['input']['project_layer_name']
    cnra_activity_layer_name =config_inputs['cnra']['input']['activity_layer_name']
    a_reference_gdb_path = config_inputs['global']['reference_gdb']
    start_year, end_year = config_inputs['global']['start_year'], config_inputs['global']['end_year']
    output_format_dict = {'start_year': start_year,
                          'end_year': end_year,
                          'date': datetime.today().strftime('%Y%m%d')}
    output_gdb_path = config_inputs['cnra']['output']['gdb_path'].format(**output_format_dict)
    output_layer_name = config_inputs['cnra']['output']['layer_name'].format(**output_format_dict)

    enrich_CNRA(cnra_input_gdb_path,
                cnra_polygon_layer_name,
                cnra_line_layer_name,
                cnra_point_layer_name,
                cnra_project_polygon_layer_name,
                cnra_activity_layer_name,
                a_reference_gdb_path,
                start_year,
                end_year,
                output_gdb_path,
                output_layer_name)

    # Get memory usage in bytes, convert to MB
    memory_usage = process.memory_info().rss / 1024 / 1024
    logger.info(f"Memory usage: {memory_usage:.2f} MB")
    

