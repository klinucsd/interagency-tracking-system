
import pandas as pd


def calculate_fiscal_years(df):
    """
    Calculate various year fields from ACTIVITY_END date.
    
    Parameters:
    df (GeoDataFrame): Input GeoDataFrame with ACTIVITY_END column
    
    Returns:
    GeoDataFrame: Input data with additional year columns
    """
    # Convert ACTIVITY_END to datetime if it isn't already
    df['ACTIVITY_END'] = pd.to_datetime(df['ACTIVITY_END'])
    
    # Calculate calendar year
    df['Year'] = df['ACTIVITY_END'].dt.year
    df['Year'] = df['Year'].fillna(0)
    df['Year'] = df['Year'].astype(int)
    
    # Convert Year to string for Year_txt
    df['Year_txt'] = df['Year'].astype(str)
    
    # Calculate Federal Fiscal Year (starts October 1)
    df['Federal_FY'] = df['ACTIVITY_END'].apply(
        lambda x: x.year + 1 if pd.notnull(x) and x.month >= 10 else x.year if pd.notnull(x) else None
    )
    
    # Calculate State Fiscal Year (starts July 1)
    df['State_FY'] = df['ACTIVITY_END'].apply(
        lambda x: x.year + 1 if pd.notnull(x) and x.month >= 7 else x.year if pd.notnull(x) else None
    )
    
    # Convert fiscal years to integer first, then to string, handling None values
    df['Federal_FY'] = df['Federal_FY'].apply(
        lambda x: str(int(x)) if pd.notnull(x) else ''
    )
    
    df['State_FY'] = df['State_FY'].apply(
        lambda x: str(int(x)) if pd.notnull(x) else ''
    )
    
    return df

