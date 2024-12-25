
import logging
import pandas as pd
import geopandas as gpd
import numpy as np
from pandas.api.types import CategoricalDtype
from typing import Dict, Tuple

from its_logging.logger_config import logger


logger = logging.getLogger('utils.assign_domains')


def create_domain_categories(excel_path: str) -> Dict[str, Tuple[CategoricalDtype, dict]]:
    """
    Creates categorical types from all sheets in an Excel file.
    Handles NULL values in 'CODE' and 'Descr' columns.
    
    Parameters:
    -----------
    excel_path : str
        Path to the Excel file
        
    Returns:
    --------
    dict
        Dictionary with sheet names as keys and tuples of (CategoricalDtype, domain_dict) as values
        where domain_dict maps codes to descriptions
    """
    # Read all sheets from Excel file
    excel = pd.ExcelFile("Domain_Tables_20231004.xlsx")
    
    # Dictionary to store categorical types and domain dictionaries
    domain_categories = {}
    
    # Process each sheet
    for sheet_name in excel.sheet_names:
        # Read the sheet
        df = pd.read_excel(excel, sheet_name=sheet_name)
        
        # Remove rows where either CODE or Descr is NULL
        df_clean = df.dropna(subset=['CODE', 'Descr'])
        
        # Create domain dictionary (CODE to Descr mapping)
        domain_dict = dict(zip(df_clean['CODE'], df_clean['Descr']))
        
        # Print warning if any rows were dropped
        rows_dropped = len(df) - len(df_clean)
        if rows_dropped > 0:
            logger.info(f"      Warning: {rows_dropped} rows with NULL values were dropped from '{sheet_name}'")
        
        # Create categorical type from codes
        if domain_dict:  # Only create if we have valid entries
            cat_type = CategoricalDtype(categories=list(domain_dict.keys()), ordered=False)
            domain_categories[sheet_name] = (cat_type, domain_dict)
            logger.info(f"      Created domain '{sheet_name}' with {len(domain_dict)} values")
        else:
            logger.info(f"      Warning: No valid entries found in sheet '{sheet_name}' after removing NULL values")
    
    return domain_categories


def apply_domain(
    gdf: gpd.GeoDataFrame, 
    column_name: str, 
    domain_name: str, 
    domains: Dict[str, Tuple[CategoricalDtype, dict]]
) -> gpd.GeoDataFrame:
    """
    Apply a domain's categorical type to a GeoDataFrame column.
    Handles NULL values in the data.
    
    Parameters:
    -----------
    gdf : geopandas.GeoDataFrame
        GeoDataFrame containing the column to be converted
    column_name : str
        Name of the column to apply the domain to
    domain_name : str
        Name of the domain to apply
    domains : dict
        Dictionary of domains created by create_domain_categories
        
    Returns:
    --------
    geopandas.GeoDataFrame
        Input GeoDataFrame with the column converted to categorical and description added        
    """
    # Validate input type
    if not isinstance(gdf, gpd.GeoDataFrame):
        raise TypeError("Input must be a GeoDataFrame")
    
    if domain_name not in domains:
        raise ValueError(f"Domain '{domain_name}' not found")

    if column_name not in gdf.columns:
        raise ValueError(f"Column '{column_name}' not found in GeoDataFrame")
        
    cat_type, domain_dict = domains[domain_name]
    
    # Create a copy of the column to avoid modifying the original
    new_col = gdf[column_name].copy()
    
    # Convert non-null values to categorical
    mask = ~gdf[column_name].isna()
    new_col[mask] = pd.Categorical(gdf.loc[mask, column_name], categories=cat_type.categories)
    gdf[column_name] = new_col
    
    # Add description column, maintaining NULL values
    # gdf[f"{column_name}_DESC"] = gdf[column_name].map(domain_dict)
    
    return gdf


def assign_project_domains(gdf, domains):
    """
    Assign multiple domains to project-related fields in a GeoDataFrame.
    
    Parameters:
    -----------
    gdf : geopandas.GeoDataFrame
        The GeoDataFrame to assign domains to
        
    Returns:
    --------
    geopandas.GeoDataFrame
        GeoDataFrame with domains applied to specified fields
    """

    logger.info(f"      Assign domains to project-related columns")
    gdf = apply_domain(gdf, "AGENCY", "D_AGENCY", domains)
    gdf = apply_domain(gdf, "ORG_ADMIN_p", "D_ORGANIZATION", domains)
    gdf = apply_domain(gdf, "ADMINISTERING_ORG", "D_ORGANIZATION", domains)
    gdf = apply_domain(gdf, "PROJECT_STATUS", "D_STATUS", domains)
    gdf = apply_domain(gdf, "PRIMARY_FUNDING_SOURCE", "D_FNDSRC", domains)
    gdf = apply_domain(gdf, "PRIMARY_FUNDING_ORG", "D_ORGANIZATION", domains)
    gdf = apply_domain(gdf, "Val_Status_p", "D_DATASTATUS", domains)
    gdf = apply_domain(gdf, "Val_Message_p", "D_VERFIEDMSG", domains)
    gdf = apply_domain(gdf, "Review_Status_p", "D_DATASTATUS", domains)
    gdf = apply_domain(gdf, "Review_Message_p", "D_VERFIEDMSG", domains)
    gdf = apply_domain(gdf, "Dataload_Status_p", "D_DATASTATUS", domains)
    gdf = apply_domain(gdf, "Dataload_Msg_p", "D_DATAMSG", domains)
    
    return gdf
    

def assign_treatment_domains(gdf, domains):
    """
    Assign multiple domains to treatment-related fields in a GeoDataFrame.
    
    Parameters:
    -----------
    gdf : geopandas.GeoDataFrame
        The GeoDataFrame to assign domains to
        
    Returns:
    --------
    geopandas.GeoDataFrame
        GeoDataFrame with domains applied to specified fields
    """

    logger.info(f"      Assign domains to treatment-related columns")
    gdf = apply_domain(gdf, "ORG_ADMIN_t", "D_ORGANIZATION", domains)
    gdf = apply_domain(gdf, "PRIMARY_OWNERSHIP_GROUP", "D_PR_OWN_GR", domains)
    gdf = apply_domain(gdf, "PRIMARY_OBJECTIVE", "D_OBJECTIVE", domains)
    gdf = apply_domain(gdf, "SECONDARY_OBJECTIVE", "D_OBJECTIVE", domains)
    gdf = apply_domain(gdf, "TERTIARY_OBJECTIVE", "D_OBJECTIVE", domains)
    gdf = apply_domain(gdf, "TREATMENT_STATUS", "D_STATUS", domains)
    gdf = apply_domain(gdf, "COUNTY", "D_CNTY", domains)
    gdf = apply_domain(gdf, "IN_WUI", "D_IN_WUI", domains)
    gdf = apply_domain(gdf, "REGION", "D_TASKFORCE", domains)
    gdf = apply_domain(gdf, "Val_Status_t", "D_DATASTATUS", domains)
    gdf = apply_domain(gdf, "Val_Message_t", "D_VERFIEDMSG", domains)
    gdf = apply_domain(gdf, "Review_Status_t", "D_DATASTATUS", domains)
    gdf = apply_domain(gdf, "Review_Message_t", "D_VERFIEDMSG", domains)
    gdf = apply_domain(gdf, "Dataload_Status_t", "D_DATASTATUS", domains)
    gdf = apply_domain(gdf, "Dataload_Msg_t", "D_DATAMSG", domains)

    return gdf
    


def assign_activity_domains(gdf, domains):
    """
    Assign multiple domains to activity-related fields in a GeoDataFrame.
    
    Parameters:
    -----------
    gdf : geopandas.GeoDataFrame
        The GeoDataFrame to assign domains to
        
    Returns:
    --------
    geopandas.GeoDataFrame
        GeoDataFrame with domains applied to specified fields
    """

    logger.info(f"      Assign domains to activity-related columns")
    gdf = apply_domain(gdf, "ORG_ADMIN_a", "D_ORGANIZATION", domains)
    gdf = apply_domain(gdf, "ACTIVITY_DESCRIPTION", "D_ACTVDSCRP", domains)
    gdf = apply_domain(gdf, "ACTIVITY_CAT", "D_ACTVCAT", domains)
    gdf = apply_domain(gdf, "BROAD_VEGETATION_TYPE", "D_BVT", domains)
    gdf = apply_domain(gdf, "BVT_USERD", "D_USERDEFINED", domains)
    gdf = apply_domain(gdf, "ACTIVITY_STATUS", "D_STATUS", domains)
    gdf = apply_domain(gdf, "ACTIVITY_UOM", "D_UOM", domains)
    gdf = apply_domain(gdf, "ADMIN_ORG_NAME", "D_ORGANIZATION", domains)
    gdf = apply_domain(gdf, "PRIMARY_FUND_SRC_NAME", "D_FNDSRC", domains)
    gdf = apply_domain(gdf, "PRIMARY_FUND_ORG_NAME", "D_ORGANIZATION", domains)
    gdf = apply_domain(gdf, "SECONDARY_FUND_SRC_NAME", "D_FNDSRC", domains)
    gdf = apply_domain(gdf, "SECONDARY_FUND_ORG_NAME", "D_ORGANIZATION", domains)
    gdf = apply_domain(gdf, "TERTIARY_FUND_SRC_NAME", "D_FNDSRC", domains)
    gdf = apply_domain(gdf, "TERTIARY_FUND_ORG_NAME", "D_ORGANIZATION", domains)
    gdf = apply_domain(gdf, "RESIDUE_FATE", "D_RESIDUEFATE", domains)
    gdf = apply_domain(gdf, "RESIDUE_FATE_UNITS", "D_UOM", domains)
    gdf = apply_domain(gdf, "VAL_STATUS_a", "D_DATASTATUS", domains)
    gdf = apply_domain(gdf, "VAL_MSG_a", "D_VERFIEDMSG", domains)
    gdf = apply_domain(gdf, "REVIEW_STATUS_a", "D_DATASTATUS", domains)
    gdf = apply_domain(gdf, "REVIEW_MSG_a", "D_VERFIEDMSG", domains)
    gdf = apply_domain(gdf, "DATALOAD_STATUS_a", "D_DATASTATUS", domains)
    gdf = apply_domain(gdf, "DATALOAD_MSG_a", "D_DATAMSG", domains)
    gdf = apply_domain(gdf, "TRMT_GEOM", "D_TRMT_GEOM", domains)
    gdf = apply_domain(gdf, "COUNTS_TO_MAS", "D_USERDEFINED", domains)

    return gdf


def assign_domains(gdf):
    domains = create_domain_categories("Domain_Tables_20231004.xlsx")
    gdf = assign_project_domains(gdf, domains)
    gdf = assign_treatment_domains(gdf, domains)
    gdf = assign_activity_domains(gdf, domains)
    return gdf


# Example usage:
if __name__ == "__main__":
    # Create domains from Excel
    domains = create_domain_categories("Domain_Tables_20231004.xlsx")

    # Print domain information
    for domain_name, (cat_type, domain_dict) in domains.items():
        print(f"\nDomain: {domain_name}")
        print("Categories:", cat_type.categories.tolist())
        print("Sample mapping:", list(domain_dict.items())[:3])


    
    # Example with GeoDataFrame
    # gdf = gpd.read_file("your_shapefile.shp")
    
    # Apply domains to multiple fields
    # gdf = apply_domain(gdf, "PRIMARY_OBJECTIVE", "D_OBJECTIVE", domains)
    # gdf = apply_domain(gdf, "SECONDARY_OBJECTIVE", "D_OBJECTIVE", domains)
    # gdf = apply_domain(gdf, "PROJECT_STATUS", "D_STATUS", domains)


