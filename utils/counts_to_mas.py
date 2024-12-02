
import logging
import geopandas as gpd
import pandas as pd
from datetime import datetime

from its_logging.logger_config import logger


logger = logging.getLogger('utils.counts_to_mas')


def counts_to_mas(gdf, start_year, end_year):
    """
    Convert activities data to Million Acre Strategy (MAS) classification using GeoPandas.
    
    Parameters:
    input_gdf (GeoDataFrame): Input geodataframe containing activity data
    
    Returns:
    GeoDataFrame: Processed geodataframe with COUNTS_TO_MAS field updated
    """
    
    logger.info("            Calculating Counts to MAS")    
    logger.info("            counts step 1/8: set to 'NO'")
    gdf['COUNTS_TO_MAS'] = 'NO'
 
    logger.info(f"            counts step 2/8: select by bounding years ({start_year}-{end_year})")
    gdf['ACTIVITY_END'] = pd.to_datetime(gdf['ACTIVITY_END'])
    mask_dates = (gdf['ACTIVITY_END'] >= f'{start_year}-01-01') & (gdf['ACTIVITY_END'] < f'{end_year+1}-01-01')
    
    logger.info("            counts step 3/8: set to 'YES' if activity description is in the list")
    qualifying_activities = [
        'BIOMASS_REMOVAL', 'BROADCAST_BURN', 'CHAIN_CRUSH', 'CHIPPING',
        'COMM_THIN', 'DISCING', 'GRP_SELECTION_HARVEST', 'HERBICIDE_APP',
        'INV_PLANT_REMOVAL', 'LANDING_TRT', 'LOP_AND_SCAT', 'MASTICATION',
        'MOWING', 'OAK_WDLND_MGMT', 'PEST_CNTRL', 'PILE_BURN', 'PILING',
        'PL_TREAT_BURNED', 'PRESCRB_HERBIVORY', 'PRUNING', 'REHAB_UNDRSTK_AREA',
        'ROAD_CLEAR', 'SANI_HARVEST', 'SINGLE_TREE_SELECTION', 'SITE_PREP',
        'SLASH_DISPOSAL', 'SP_PRODUCTS', 'THIN_MAN', 'THIN_MECH',
        'TRANSITION_HARVEST', 'TREE_FELL', 'TREE_PLNTING', 'TREE_RELEASE_WEED',
        'TREE_SEEDING', 'UTIL_RIGHTOFWAY_CLR', 'VARIABLE_RETEN_HARVEST', 'YARDING'
    ]
    
    # Apply all conditions sequentially
    mask_activities = gdf['ACTIVITY_DESCRIPTION'].isin(qualifying_activities)
    
    logger.info("            counts step 4/8: set to 'NO' if not 'Acres'")
    mask_uom = gdf['ACTIVITY_UOM'] == 'AC'
    
    logger.info("            counts step 5/8: set to 'NO' if status is 'Canceled', 'Planned', 'Outyear', or 'Proposed'")
    excluded_statuses = ['CANCELLED', 'PLANNED', 'OUTYEAR', 'PROPOSED']
    mask_status = ~gdf['ACTIVITY_STATUS'].isin(excluded_statuses)
    
    logger.info("            counts step 6/8: set to 'NO' if Activity Category is 'Watershed Improvement'")
    mask_category = gdf['ACTIVITY_CAT'] != 'WATSHD_IMPRV'
    
    logger.info("            counts step 7/8: set to 'NO' if Agency is 'Other' and Admin is 'CARB'")
    mask_pifirs = ~((gdf['AGENCY'] == 'OTHER') & (gdf['ORG_ADMIN_p'] == 'CARB'))
    
    logger.info("            counts step 8/8: set to 'NO' if Org is 'USFS' and Status is 'Active'")
    mask_usfs = ~((gdf['ADMINISTERING_ORG'] == 'USFS') & (gdf['ACTIVITY_STATUS'] == 'ACTIVE'))
    
    excluded_agencies = ['BOF', 'CCC', 'SMMC', 'SNC', 'SCC', 'SDRC', 'MRCA', 'RMC', 'OTHER']
    mask_agencies = ~gdf['ADMINISTERING_ORG'].isin(excluded_agencies)
    
    final_mask = (mask_dates & mask_activities & mask_uom & mask_status & 
                 mask_category & mask_pifirs & mask_usfs & mask_agencies)
    
    gdf.loc[final_mask, 'COUNTS_TO_MAS'] = 'YES'
    
    return gdf
