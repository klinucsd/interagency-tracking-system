
import os
import logging
import pyogrio
import pandas as pd
import geopandas as gpd

from its_logging.logger_config import logger
from utils.gdf_utils import show_columns
from utils.keep_fields import keep_fields
from utils.category import categorize_activity
from utils.standardize_domains import standardize_domains
from utils.counts_to_mas import counts_to_mas


logger = logging.getLogger('utils.crosswalk')


def crosswalk(input_df, a_reference_gdb_path, start_year, end_year):
    """
    Crosswalk activities and enrich data with additional fields.
    
    Parameters:
    input_df (GeoDataFrame): Input GeoDataFrame
    crosswalk_table_path (str): Path to the crosswalk table
    
    Returns:
    GeoDataFrame: Processed and enriched data
    """
    logger.info("         Calculating Crosswalking Activites...")
    
    logger.info("            Load Crosswalk table...")
    crosswalk_table = pyogrio.read_dataframe(a_reference_gdb_path, layer='Crosswalk')
    show_columns(logger, crosswalk_table, "crosswalk_table")
    
    logger.info("            cross step 1/8 add join")
    # First, do the calculations on matched rows
    merged_df = input_df.merge(
        crosswalk_table,
        left_on='Crosswalk',
        right_on='Original_Activity',
        how='left'  # KEEP_COMMON equivalent
    )

    logger.info("            cross step 2/8 calculate activities")
    mask = merged_df['Activity'].notna()
    merged_df.loc[mask, 'ACTIVITY_DESCRIPTION'] = merged_df.loc[mask, 'Activity']
    
    logger.info("            cross step 3/8 calculate residue fate field")
    merged_df.loc[mask, 'RESIDUE_FATE'] = merged_df.loc[mask, 'Residue_Fate']

    logger.info("            cross step 4/8 select attribute by layer")
    objective_mask = merged_df['PRIMARY_OBJECTIVE'].isna() | (merged_df['PRIMARY_OBJECTIVE'] == 'TBD')

    logger.info("            cross step 5/8 calculating objective...")
    update_mask = mask & objective_mask
    merged_df.loc[update_mask, 'PRIMARY_OBJECTIVE'] = merged_df.loc[update_mask, 'Objective']

    # Remove joined columns except those we want to keep
    columns_to_keep = [col for col in input_df.columns]
    df_no_join = merged_df[columns_to_keep]
    show_columns(logger, df_no_join, "df_no_join")
    
    logger.info("            cross step 6/8 calculate category")
    categorized_df = categorize_activity(df_no_join)
    logger.debug(f"categorized_df['ACTIVITY_CAT'] : {categorized_df['ACTIVITY_CAT'].unique()}")
    
    logger.info("            cross step 7/8 standardize domains")
    standardized_df = standardize_domains(categorized_df)
    
    logger.info("            cross step 8/8 counts towards MAS")
    standardized_df = counts_to_mas(standardized_df, start_year, end_year)
    
    # Keep only specified fields
    final_output = keep_fields(standardized_df)
    show_columns(logger, final_output, "final_output")

    return final_output

