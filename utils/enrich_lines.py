
import os
import time
import logging
import pandas as pd
import geopandas as gpd
from datetime import datetime
import numpy as np

from its_logging.logger_config import logger
from utils.gdf_utils import show_columns
from utils.keep_fields import keep_fields
from utils.year import calculate_fiscal_years
from utils.crosswalk import crosswalk
from utils.its_utils import get_wfr_tf_template
from utils.enrich_points import enrich_points


# logger = logging.getLogger('utils.enrich_lines')
logger = logging.getLogger(__name__)


def enrich_lines(line_gdf, a_reference_gdb_path, start_year, end_year):

    pd.options.display.float_format = '{:.15f}'.format

    logger.info("      Executing Line Enrichments...")
    line_gdf = line_gdf.copy()

    # Read template if it's needed for schema alignment
    template_gdf = get_wfr_tf_template(a_reference_gdb_path)

    # Step 1: Convert lines to points (centroids)
    logger.info("         enrich line step 1/4 convert to points")
    points_gdf = line_gdf.copy()
    points_gdf.geometry = points_gdf.representative_point()
    
    # Step 2: Enrich points
    logger.info("         enrich line step 2/4 execute enrich_points...")
    enriched_points = enrich_points(points_gdf, a_reference_gdb_path, start_year, end_year)

    
    # Step 3: Join enriched attributes back to lines
    logger.info("         enrich line step 3/4 importing attributes")

    # Merge enriched point attributes back to lines
    merged_gdf = line_gdf.merge(
        enriched_points.drop(columns=['geometry']),
        on='ACTIVID_USER',
        how='left',
        suffixes=('', '_1')
    )

    # Update fields with enriched values
    fields_to_update = [
        'LATITUDE', 'LONGITUDE', 'PRIMARY_OWNERSHIP_GROUP', 'PRIMARY_OBJECTIVE',
        'COUNTY', 'IN_WUI', 'REGION', 'ACTIVITY_DESCRIPTION',
        'BROAD_VEGETATION_TYPE', 'BVT_USERD', 'ACTIVITY_CAT', 'RESIDUE_FATE',
        'Year', 'Federal_FY', 'State_FY', 'COUNTS_TO_MAS'
    ]

    for field in fields_to_update:
        if f'{field}_1' in merged_gdf.columns:
            merged_gdf[field] = merged_gdf[f'{field}_1']
            merged_gdf = merged_gdf.drop(columns=[f'{field}_1'])

    # Add specific fields
    merged_gdf['Year_txt'] = merged_gdf['Year'].astype(str)
    merged_gdf['TRMT_GEOM'] = 'LINE'

    # Step 4: Align to template
    logger.info("         enrich line step 4/4 align to template")
    # Ensure the output has the same schema as the template
    for col in template_gdf.columns:
        if col not in merged_gdf.columns and col != 'geometry':
            merged_gdf[col] = None

    # Set the correct CRS
    merged_gdf = merged_gdf.to_crs("EPSG:3310")  # NAD 1983 California (Teale) Albers
    
    return merged_gdf




