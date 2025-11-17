import os
import numpy as np
import pandas as pd
import geopandas as gpd
import sys
import yaml
sys.path.append('../')

from utils.save_gdf_to_gdb import save_gdf_to_gdb


def check_valid(enriched_points, enriched_lines, enriched_polygons):
    assert np.all(enriched_points.is_valid)
    assert np.all(enriched_lines.is_valid)
    assert np.all(enriched_polygons.is_valid)


def check_admin_org_null(enriched_points, enriched_lines, enriched_polygons):
    # Admin org must be populated if counts to mas
    assert len(enriched_points[(enriched_points.COUNTS_TO_MAS=="YES") & (enriched_points.ADMINISTERING_ORG.isna())]) == 0
    assert len(enriched_lines[(enriched_lines.COUNTS_TO_MAS=="YES") & (enriched_lines.ADMINISTERING_ORG.isna())]) == 0
    assert len(enriched_polygons[(enriched_polygons.COUNTS_TO_MAS=="YES") & (enriched_polygons.ADMINISTERING_ORG.isna())]) == 0


def check_agency_null(enriched_points, enriched_lines, enriched_polygons):
    # Agency must be populated if counts to mas
    assert len(enriched_points[(enriched_points.COUNTS_TO_MAS=="YES") & (enriched_points.AGENCY.isna())]) == 0
    assert len(enriched_lines[(enriched_lines.COUNTS_TO_MAS=="YES") & (enriched_lines.AGENCY.isna())]) == 0
    assert len(enriched_polygons[(enriched_polygons.COUNTS_TO_MAS=="YES") & (enriched_polygons.AGENCY.isna())]) == 0


def check_region_null(enriched_points, enriched_lines, enriched_polygons):
    # Region, vegetation type, ownership must be populated 
    assert len(enriched_points[enriched_points.REGION.isna()]) == 0
    assert len(enriched_lines[enriched_lines.REGION.isna()]) == 0
    assert len(enriched_polygons[enriched_polygons.REGION.isna()]) == 0


def check_veg_null(enriched_points, enriched_lines, enriched_polygons):
    # Region, vegetation type, ownership must be populated 
    assert len(enriched_points[enriched_points.BROAD_VEGETATION_TYPE.isna()]) == 0
    assert len(enriched_lines[enriched_lines.BROAD_VEGETATION_TYPE.isna()]) == 0
    assert len(enriched_polygons[enriched_polygons.BROAD_VEGETATION_TYPE.isna()]) == 0


def check_ownership_null(enriched_points, enriched_lines, enriched_polygons):
    # Region, vegetation type, ownership must be populated 
    assert len(enriched_points[enriched_points.PRIMARY_OWNERSHIP_GROUP.isna()]) == 0
    assert len(enriched_lines[enriched_lines.PRIMARY_OWNERSHIP_GROUP.isna()]) == 0
    assert len(enriched_polygons[enriched_polygons.PRIMARY_OWNERSHIP_GROUP.isna()]) == 0


def check_active_status(enriched_points, enriched_lines, enriched_polygons):
    # only CNRA is allowed to have ACTIVE status
    assert enriched_points[(enriched_points.COUNTS_TO_MAS == 'YES') & (enriched_points.ACTIVITY_STATUS == 'ACTIVE')].AGENCY.unique()) == 1
    assert enriched_points[(enriched_points.COUNTS_TO_MAS == 'YES') & (enriched_points.ACTIVITY_STATUS == 'ACTIVE')].AGENCY.unique()[0] == 'CNRA'
    assert len(enriched_lines[(enriched_lines.COUNTS_TO_MAS == 'YES') & (enriched_lines.ACTIVITY_STATUS == 'ACTIVE')].AGENCY.unique()) == 1
    assert enriched_lines[(enriched_lines.COUNTS_TO_MAS == 'YES') & (enriched_lines.ACTIVITY_STATUS == 'ACTIVE')].AGENCY.unique()[0] == 'CNRA'
    assert len(enriched_polygons[(enriched_polygons.COUNTS_TO_MAS == 'YES') & (enriched_polygons.ACTIVITY_STATUS == 'ACTIVE')].AGENCY.unique()) == 1
    assert enriched_polygons[(enriched_polygons.COUNTS_TO_MAS == 'YES') & (enriched_polygons.ACTIVITY_STATUS == 'ACTIVE')].AGENCY.unique()[0] == 'CNRA'
    

def check_activity_null(enriched_points, enriched_lines, enriched_polygons):
    # Activity category must be populated 
    assert len(enriched_lines[enriched_lines.ACTIVITY_CAT.isna()]) == 0
    assert len(enriched_points[enriched_points.ACTIVITY_CAT.isna()]) == 0
    assert len(enriched_polygons[enriched_polygons.ACTIVITY_CAT.isna()]) == 0
    # Activity category with NOT_DEFINED can only come from activity description of TBD or NOT_DEFINED
    assert set(enriched_points[enriched_points.ACTIVITY_CAT == 'NOT_DEFINED'].ACTIVITY_DESCRIPTION.unique()).issubset(set(['TBD', 'NOT_DEFINED']))
    assert set(enriched_lines[enriched_lines.ACTIVITY_CAT == 'NOT_DEFINED'].ACTIVITY_DESCRIPTION.unique()).issubset(set(['TBD', 'NOT_DEFINED']))
    assert set(enriched_polygons[enriched_polygons.ACTIVITY_CAT == 'NOT_DEFINED'].ACTIVITY_DESCRIPTION.unique()).issubset(set(['TBD', 'NOT_DEFINED']))


def thresh_null_ratio(enriched_points, enriched_lines, enriched_polygons, na_thresh = 0.01):
    # Threshold of na %
    cols =["AGENCY",
        "ADMINISTERING_ORG",
        "PRIMARY_OWNERSHIP_GROUP",
        "COUNTY",
        "REGION",
        "ACTIVITY_DESCRIPTION",
        "ACTIVITY_CAT",
        "BROAD_VEGETATION_TYPE",
        "ACTIVITY_STATUS",
        "ACTIVITY_QUANTITY",
        "ACTIVITY_UOM",
        "ACTIVITY_END",
        "Year_txt"]
    
    for col in cols:
        assert sum(enriched_lines[col].isna())/len(enriched_lines) < na_thresh
        assert sum(enriched_points[col].isna())/len(enriched_points) < na_thresh
        assert sum(enriched_polygons[col].isna())/len(enriched_polygons) < na_thresh


if __name__ == "__main__":

    with open("..\config.yaml", 'r') as stream:
        config_inputs = yaml.safe_load(stream)
    
    append_path = config_inputs['appended']['gdb_path']


    enriched_polygons = gpd.read_file(append_path, driver='OpenFileGDB', layer='appended_poly')
    enriched_points = gpd.read_file(append_path, driver='OpenFileGDB', layer='appended_point')
    enriched_lines = gpd.read_file(append_path, driver='OpenFileGDB', layer='appended_line')

    check_valid(enriched_points, enriched_lines, enriched_polygons)
    check_admin_org_null(enriched_points, enriched_lines, enriched_polygons)
    check_agency_null(enriched_points, enriched_lines, enriched_polygons)
    check_region_null(enriched_points, enriched_lines, enriched_polygons)
    check_veg_null(enriched_points, enriched_lines, enriched_polygons)
    check_veg_null(enriched_points, enriched_lines, enriched_polygons)
    check_ownership_null(enriched_points, enriched_lines, enriched_polygons)
    check_active_status(enriched_points, enriched_lines, enriched_polygons)
    check_activity_null(enriched_points, enriched_lines, enriched_polygons)
    thresh_null_ratio(enriched_points, enriched_lines, enriched_polygons)