
import warnings
import logging
import time
import psutil
import os

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


logger = logging.getLogger('enrich.enrich_NFPORS')

# Suppress pyogrio INFO logs
logging.getLogger("pyogrio").setLevel(logging.WARNING)

# Suppress specific warnings
warnings.filterwarnings("ignore", category=RuntimeWarning)

# Suppress specific FutureWarning messages
warnings.simplefilter(action='ignore', category=FutureWarning)


NFPORS_POLYGON_COLUMNS = [
    'trt_unt_id', 'local_id', 'col_date', 'trt_status', 'col_meth', 'comments', 'gis_acres',
    'pstatus', 'modifiedon', 'createdon', 'cent_lat', 'cent_lon', 'userid', 'st_abbr',
    'cong_dist', 'cnty_fips', 'trt_nm', 'fy', 'plan_acc_ac', 'act_acc_ac', 'act_init_dt',
    'act_comp_dt', 'nfporsfid', 'trt_id_db', 'type_name', 'cat_nm', 'trt_statnm',
    'col_methnm', 'plan_int_dt', 'unit_id', 'agency', 'trt_id', 'created_by', 'edited_by',
    'projectname', 'regionname', 'projectid', 'keypointarea', 'unitname', 'deptname',
    'countyname', 'statename', 'regioncode', 'districtname', 'isbil', 'bilfunding', 'geometry'
]


NFPORS_BIA_COLUMNS = [
    'treatmentname', 'unitid', 'unitname', 'regionname', 'agencyname', 'deptname',
    'countyname', 'statename', 'districtname', 'representative', 'keypointname', 'latitude',
    'longitude', 'wuiid', 'plannedfy', 'initiatedfy', 'completedfy', 'plannedinitiationdate',
    'actualinitiationdate', 'actualcompletiondate', 'categoryname', 'plannedaccomplishment',
    'fyaccomplishment', 'totalaccomplishment', 'symbol', 'treatmentcreated', 'treatmentmodified',
    'typename', 'source', 'treatmentid', 'projectname', 'activitytreatapprvd', 'localapprvdt',
    'regionalapprvdt', 'bureauapprvdt', 'unitofmeas', 'plancontrcost', 'plandirectcost',
    'treatmentowner', 'nepatype', 'movedbetcondclassacres', 'regioncode', 'projectid',
    'treatmentlocalidentifier', 'obligationfiscalyear', 'actualcompletionyear',
    'activitytreatmentnotes', 'iswui', 'iscompleted', 'bureaurecguid', 'isbil',
    'geometry'
]


NFPORS_FWS_COLUMNS = [
    'treatmentname', 'unitid', 'unitname', 'regionname', 'agencyname', 'deptname', 'countyname',
    'statename', 'districtname', 'representative', 'keypointname', 'latitude', 'longitude',
    'wuiid', 'plannedfy', 'initiatedfy', 'completedfy', 'plannedinitiationdate',
    'actualinitiationdate', 'actualcompletiondate', 'categoryname', 'plannedaccomplishment',
    'fyaccomplishment', 'totalaccomplishment', 'symbol', 'treatmentcreated', 'treatmentmodified',
    'typename', 'source', 'treatmentid', 'projectname', 'activitytreatapprvd', 'localapprvdt',
    'regionalapprvdt', 'bureauapprvdt', 'unitofmeas', 'plancontrcost', 'plandirectcost',
    'treatmentowner', 'nepatype', 'movedbetcondclassacres', 'regioncode', 'projectid',
    'treatmentlocalidentifier', 'obligationfiscalyear', 'actualcompletionyear',
    'activitytreatmentnotes', 'iswui', 'iscompleted', 'bureaurecguid', 'isbil',
    'geometry'
]

def standardize_NFPORS_polygon(nfpors_polygon_gdf, a_reference_gdb_path, start_year, end_year, output_gdb_path, output_layer_name):
    """
    Standardize polygon features using GeoPandas
    """
    logger.info("Performing Polygons Standardization")
    
    ### UPDATE: remove NPS due to NPS having independent dataset now
    logger.info("   step 1/11 select by DOI agency (BIA, FWS, NPS)")
    nfpors_polygon_gdf = nfpors_polygon_gdf[nfpors_polygon_gdf['agency'].isin(['BIA', 'FWS'])]
    
    logger.info("   step 2/11 select after 1995")

    # Convert act_comp_dt to datetime if it's not already
    nfpors_polygon_gdf.loc[:, 'act_comp_dt'] = pd.to_datetime(nfpors_polygon_gdf['act_comp_dt'])

    # Filter the data
    nfpors_polygon_gdf = nfpors_polygon_gdf[(nfpors_polygon_gdf['act_comp_dt'] >= '1995-01-01') | (nfpors_polygon_gdf['act_comp_dt'].isna())]
    
    logger.info("   step 3/11 select CA")
    nfpors_polygon_gdf = nfpors_polygon_gdf[nfpors_polygon_gdf['st_abbr'] == 'CA']
    
    logger.info("   step 4/11 repair geometry")
    nfpors_polygon_gdf = repair_geometries(nfpors_polygon_gdf)
      
    logger.info("   step 5/11 rename and add fields")
    nfpors_polygon_gdf = nfpors_polygon_gdf.rename(columns={'agency': 'agency_'})
    nfpors_polygon_gdf = add_common_columns(nfpors_polygon_gdf)
    show_columns(logger, nfpors_polygon_gdf, "nfpors_polygon_gdf")    

    logger.info("   step 6/11 import attributes")
    # Calculate fields
    nfpors_polygon_gdf['PROJECTID_USER'] = 'NFPORS' + nfpors_polygon_gdf['trt_id'].astype(str)
    nfpors_polygon_gdf['AGENCY'] = 'DOI'
    nfpors_polygon_gdf['ADMINISTERING_ORG'] = nfpors_polygon_gdf['agency_']
    nfpors_polygon_gdf['IMPLEMENTING_ORG'] = nfpors_polygon_gdf['agency_']
    nfpors_polygon_gdf['ACTIVITY_NAME'] = nfpors_polygon_gdf['trt_nm']
    nfpors_polygon_gdf['ACTIVITY_UOM'] = 'AC'
    
    # Handle activity quantity
    nfpors_polygon_gdf['ACTIVITY_QUANTITY'] = nfpors_polygon_gdf['gis_acres']
    
    nfpors_polygon_gdf['ACTIVITY_START'] = nfpors_polygon_gdf['modifiedon']
    
    # Handle activity end date
    nfpors_polygon_gdf['ACTIVITY_END'] = nfpors_polygon_gdf.apply(
        lambda x: x['act_comp_dt'] if pd.notnull(x['act_comp_dt']) else x['modifiedon'],
        axis=1
    )

    def get_poly_status(status_col):
        if status_col == "Accomplished":
            return "COMPLETE"
        elif status_col == "Initiated":
            return "ACTIVE"
        else:
            return status_col
    
    nfpors_polygon_gdf['ACTIVITY_STATUS'] = nfpors_polygon_gdf['trt_statnm'].apply(get_poly_status)
    nfpors_polygon_gdf['Source'] = 'nfpors_haz_fuels_treatments_reduction'
    nfpors_polygon_gdf['Crosswalk'] = nfpors_polygon_gdf['type_name']
    
    logger.info("   step 7/11 keep only necessary fields")
    nfpors_polygon_gdf = keep_fields(nfpors_polygon_gdf)
    show_columns(logger, nfpors_polygon_gdf, "nfpors_polygon_gdf")
        
    logger.info("      Standardization Complete")
    logger.info(f"        standardized has {len(nfpors_polygon_gdf)} records")    

    logger.info("   step 8/11 enriching Polygons...")
    nfpors_polygon_gdf = enrich_polygons(nfpors_polygon_gdf, a_reference_gdb_path, start_year, end_year) 
    logger.info("      enriched has {} records".format(nfpors_polygon_gdf.shape[0]))

    logger.info("   step 9/11 add project user ID")
    nfpors_polygon_gdf['TRMTID_USER'] = (
        nfpors_polygon_gdf['PROJECTID_USER'].astype(str).str[-7:] + '-' + 
        nfpors_polygon_gdf['IN_WUI'].astype(str).str[:3] + '-' + 
        nfpors_polygon_gdf['PRIMARY_OWNERSHIP_GROUP'].astype(str).str[:1] + '-' + 
        nfpors_polygon_gdf['COUNTY'].astype(str).str[:8] + '-' + 
        nfpors_polygon_gdf['PRIMARY_OBJECTIVE'].astype(str).str[:12]
    )

    logger.info("   step 10/11 assign Domains...")
    nfpors_polygon_gdf = assign_domains(nfpors_polygon_gdf)


    # UPDATE: FWS polygon record have faulty data that each part of a TRMTID_USER/PROJECTID_USER record 
    # that is a multi-part polygon is being logged as a unique activity, but the ACTIVITY_QUANTITY is 
    # being reported as the sum of the multipart polygon acres for each record rather than only that 
    # part of the multipart polygon. 
    # Solution: use polygon GIS area for ACTIVITY QUANTITY
    nfpors_polygon_gdf['ACTIVITY_QUANTITY'] = nfpors_polygon_gdf['TREATMENT_AREA']
    nfpors_polygon_gdf['AGENCY'] = 'DOI'

    # fiscal cutoff for new IFPIRS 
    # BLM, NPS, NFPORS after 2023/10/01 ACTIVITY START will be reported by IFPIRS hence not count to MAS
    nfpors_polygon_gdf.loc[nfpors_polygon_gdf['ACTIVITY_END'] >= f'2023-10-01', 'COUNTS_TO_MAS'] = 'NO'  
    

    logger.info("   step 11/11 Save enriched polygons...")
    save_gdf_to_gdb(nfpors_polygon_gdf,
                    output_gdb_path,
                    f"{output_layer_name}_polygon",
                    group_name="c_Enriched")
    
    return nfpors_polygon_gdf
    

def standardize_NFPORS_point(bia_gdf, fws_gdf, a_reference_gdb_path, start_year, end_year, output_gdb_path, output_layer_name):
    """
    Standardize point features using GeoPandas
    """
    logger.info("Performing Points Standardization")

    logger.info("   step 1/10 select BIA points in CA")
    bia_pts_ca = bia_gdf[bia_gdf['statename'] == 'California'].copy()
    logger.info(f"      BIA selected points has {len(bia_pts_ca)} records")
    
    logger.info("   step 2/10 select FWS points in CA")
    fws_pts_ca = fws_gdf[fws_gdf['statename'] == 'California'].copy()
    logger.info(f"      FWS selected points has {len(fws_pts_ca)} records")
    
    logger.info("   step 3/10 combine points layers")
    combined_pts = pd.concat([bia_pts_ca, fws_pts_ca], ignore_index=True)
    logger.info(f"      appended points has {len(combined_pts)} records")

    logger.info("   step 4/10 rename and add fields")
    # Rename fields
    field_renames = {
        'source': 'source_',
        'projectid': 'project_id',
        'latitude': 'latitude_',
        'longitude': 'longitude_'
    }
    combined_pts = combined_pts.rename(columns=field_renames)
    combined_pts = add_common_columns(combined_pts)
    show_columns(logger, combined_pts, "combined_pts")        
    
    logger.info("   step 5/10 import attributes")
    # Calculate fields
    combined_pts['PROJECTID_USER'] = 'NFPORS' + combined_pts['project_id'].astype(str)
    combined_pts['AGENCY'] = 'DOI'
    combined_pts['ADMINISTERING_ORG'] = combined_pts['agencyname']
    combined_pts['IMPLEMENTING_ORG'] = combined_pts['agencyname']
    
    # Handle WUI calculation
    def wui_converter(wui):
        if wui == 'Y':
            return 'WUI_USER_DEFINED'
        elif wui == 'N':
            return 'NON-WUI_USER_DEFINED'
        return wui
    
    combined_pts['IN_WUI'] = combined_pts['iswui'].apply(wui_converter)
    combined_pts['ACTIVITY_NAME'] = combined_pts['treatmentname']
    combined_pts['ACTIVITY_UOM'] = combined_pts['unitofmeas']
    
    # Handle activity quantity
    combined_pts['ACTIVITY_QUANTITY'] = combined_pts.apply(
        lambda x: x['plannedaccomplishment'] if x['totalaccomplishment'] == 0 else x['totalaccomplishment'],
        axis=1
    )
    
    # Handle treatment area
    combined_pts['TREATMENT_AREA'] = combined_pts.apply(
        lambda x: x['ACTIVITY_QUANTITY'] if x['ACTIVITY_UOM'] == 'AC' else None,
        axis=1
    )
    
    # Handle dates
    def handle_date(actual_date, planned_date):
        # Convert dates to timezone-naive datetime objects for comparison
        if pd.notnull(actual_date):
            actual_dt = pd.to_datetime(actual_date).tz_localize(None)
            cutoff_dt = pd.to_datetime('1901-01-01')
            if actual_dt >= cutoff_dt:
                return actual_date
        return planned_date

    # Then use this updated function in apply:
    combined_pts['ACTIVITY_START'] = combined_pts.apply(
        lambda x: handle_date(x['actualinitiationdate'], x['plannedinitiationdate']),
        axis=1
    )


    # TODO: NA if not actual compeleted
    combined_pts['ACTIVITY_END'] = combined_pts['actualcompletiondate']
    
    # ACTIVITY_STATUS dependent on 3 planned date, actual start date, actual end date

    # Default to have a valid planned date from source dataset

    combined_pts['ACTIVITY_STATUS'] = 'PLANNED'
    # Active filter
    # NFPORS default NA for date is 1900-01-01
    na_dt = pd.to_datetime('1901-01-01')
    mask_active = combined_pts['actualinitiationdate'].apply(lambda x: pd.to_datetime(x).tz_localize(None) >= na_dt)
    combined_pts.loc[mask_active, 'ACTIVITY_STATUS'] = 'ACTIVE'

    # Complete filter
    combined_pts.loc[combined_pts['iscompleted'] == '1', 'ACTIVITY_STATUS'] = 'COMPLETE'

    combined_pts['Source'] = 'nfpors_current_fy_treatments'
    combined_pts['Crosswalk'] = combined_pts['typename']
    combined_pts['TRMT_GEOM'] = 'POINT'
    
    logger.info("   step 6/10 keep only necessary fields")
    combined_pts = keep_fields(combined_pts)
    show_columns(logger, combined_pts, "combined_pts")
    
    logger.info("   step 7/10 enriching Points...")
    nfpors_point_gdf = enrich_points(combined_pts, a_reference_gdb_path, start_year, end_year) 
    logger.info("            enriched has {} records".format(nfpors_point_gdf.shape[0]))    

    logger.info("   step 8/10 add project user ID")
    nfpors_point_gdf['TRMTID_USER'] = (
        nfpors_point_gdf['PROJECTID_USER'].str[-7:] + '-' + 
        nfpors_point_gdf['IN_WUI'].str[:3] + '-' + 
        nfpors_point_gdf['PRIMARY_OWNERSHIP_GROUP'].str[:1] + '-' + 
        nfpors_point_gdf['COUNTY'].str[:8] + '-' + 
        nfpors_point_gdf['PRIMARY_OBJECTIVE'].str[:12]
    )

    logger.info("   step 9/10 assign Domains...")
    nfpors_point_gdf = assign_domains(nfpors_point_gdf)    

    # fiscal cutoff for new IFPIRS 
    # BLM, NPS, NFPORS after 2023/10/01 ACTIVITY START will be reported by IFPIRS hence not count to MAS
    nfpors_point_gdf.loc[nfpors_point_gdf['ACTIVITY_END'] >= f'2023-10-01', 'COUNTS_TO_MAS'] = 'NO'  


    logger.info("   step 10/10 Save enriched points...")
    save_gdf_to_gdb(nfpors_point_gdf,
                    output_gdb_path,
                    f"{output_layer_name}_point",
                    group_name="c_Enriched")
    
    return nfpors_point_gdf


def enrich_NFPORS(nfpors_gdb_path,
                  nfpors_polygon_layer_name,
                  nfpors_bia_layer_name,
                  nfpors_fws_layer_name,
                  a_reference_gdb_path,
                  start_year,
                  end_year,
                  output_gdb_path,
                  output_layer_name):

    logger.info("Load the NFPORS data into GeoDataFrames")
    start = time.time()

    ### Load the polygon layer
    nfpors_polygon = gpd.read_file(nfpors_gdb_path, driver="OpenFileGDB", layer=nfpors_polygon_layer_name)
    logger.info(f"   time for loading {nfpors_polygon_layer_name}: {time.time()-start}")

    # remap column names in 2024 dataset shp abbreviation
    nfpors_polygon.columns = [c.lower() for c in nfpors_polygon.columns]
    remap_dict = {'plan_acc_a': 'plan_acc_ac',
                'act_init_d': 'act_init_dt',
                'act_comp_d': 'act_comp_dt',
                'plan_int_d': 'plan_int_dt',
                'projectnam': 'projectname',
                'keypointar': 'keypointarea',
                'districtna': 'districtname'
                }
    nfpors_polygon = nfpors_polygon.rename(remap_dict, axis=1)
    # validate the polygon data
    verify_gdf_columns(nfpors_polygon, NFPORS_POLYGON_COLUMNS, logger)
    
    nfpors_polygon = nfpors_polygon.to_crs(3310)
    show_columns(logger, nfpors_polygon, "nfpors_polygon")

    ### Load the bia layer
    nfpors_bia = gpd.read_file(nfpors_gdb_path, driver="OpenFileGDB", layer=nfpors_bia_layer_name)
    logger.info(f"   time for loading {nfpors_bia_layer_name}: {time.time()-start}")


    ### if fws is not provided, assume bia/fws is contained in the same input dataset
    if nfpors_fws_layer_name == None:

        nfpors_bia.columns = [c.lower() for c in nfpors_bia.columns]
        remap_dict={'treatmentn': 'treatmentname',
            'districtna': 'districtname',
            'representa': 'representative',
            'keypointna': 'keypointname',
            'initiatedf': 'initiatedfy',
            'completedf': 'completedfy',
            'plannedini': 'plannedinitiationdate',
            'actualinit': 'actualinitiationdate',
            'actualcomp': 'actualcompletiondate',
            'categoryna': 'categoryname',
            'plannedacc': 'plannedaccomplishment',
            'fyaccompli': 'fyaccomplishment',
            'totalaccom': 'totalaccomplishment',
            'treatmentc': 'treatmentcreated',
            'treatmentm': 'treatmentmodified',
            'treatmenti': 'treatmentid',
            'projectnam': 'projectname',
            'activitytr': 'activitytreatapprvd',
            'localapprv': 'localapprvdt',
            'regionalap': 'regionalapprvdt',
            'bureauappr': 'bureauapprvdt',
            'plancontrc': 'plancontrcost',
            'plandirect': 'plandirectcost',
            'treatmento': 'treatmentowner',
            'movedbetco': 'movedbetcondclassacres',
            'treatmentl': 'treatmentlocalidentifier',
            'obligation': 'obligationfiscalyear',
            'iscomplete': 'iscompleted',
            'bureaurecg': 'bureaurecguid',
            'actualco_1': 'actualcompletionyear',
            'activity_1': 'activitytreatmentnotes'}
        nfpors_bia = nfpors_bia.rename(remap_dict, axis=1)
    
    # validate the bia data
    verify_gdf_columns(nfpors_bia, NFPORS_BIA_COLUMNS, logger)

 
    
    nfpors_bia = nfpors_bia.to_crs(3310)
    show_columns(logger, nfpors_bia, "nfpors_bia")
    
    if nfpors_fws_layer_name == None:
        nfpors_fws =  pd.DataFrame(columns=nfpors_bia.columns)
    else:
        ### Load the fws layer
        nfpors_fws = gpd.read_file(nfpors_gdb_path, driver="OpenFileGDB", layer=nfpors_fws_layer_name)
        logger.info(f"   time for loading {nfpors_fws_layer_name}: {time.time()-start}")

        # validate the fws data
        verify_gdf_columns(nfpors_fws, NFPORS_FWS_COLUMNS, logger)
        
        nfpors_fws = nfpors_fws.to_crs(3310)
        show_columns(logger, nfpors_fws, "nfpors_fws")

    standardize_NFPORS_polygon(nfpors_polygon, a_reference_gdb_path, start_year, end_year, output_gdb_path, output_layer_name)
    standardize_NFPORS_point(nfpors_bia, nfpors_fws, a_reference_gdb_path, start_year, end_year, output_gdb_path, output_layer_name)


    
if __name__ == "__main__":
    # Get the current process ID
    process = psutil.Process(os.getpid())

    nfpors_input_gdb_path = "b_Originals/NFPORS_2023_20240624_ServiceDownload.gdb"
    nfpors_polygon_layer_name = "NFPORS_2023_20240619_Fuel_Treatment_Polygons_ServiceDownload"
    nfpors_bia_layer_name = "NFPORS_2023_20240619_Current_FY_Treatments_BIA_ServiceDownload"
    nfpors_fws_layer_name = "NFPORS_2023_20240619_Current_FY_Treatments_FWS_ServiceDownload"
    a_reference_gdb_path = "a_Reference.gdb"
    start_year, end_year = 2010, 2025
    output_gdb_path = f"/tmp/NFPORS_{start_year}_{end_year}.gdb"
    output_layer_name = f"NFPORS_enriched_{datetime.today().strftime('%Y%m%d')}"

    enrich_NFPORS(nfpors_input_gdb_path,
                  nfpors_polygon_layer_name,
                  nfpors_bia_layer_name,
                  nfpors_fws_layer_name,
                  a_reference_gdb_path,
                  start_year,
                  end_year,
                  output_gdb_path,
                  output_layer_name)

    # Get memory usage in bytes, convert to MB
    memory_usage = process.memory_info().rss / 1024 / 1024
    logger.info(f"Memory usage: {memory_usage:.2f} MB")
    
