import geopandas as gpd
import sys
sys.path.append('../')

from utils.save_gdf_to_gdb import save_gdf_to_gdb
from datetime import datetime

def get_activity_report(enriched_points, enriched_lines, enriched_polygons):
    append_all = pd.concat([enriched_lines, enriched_points, enriched_polygons])
    append_all = append_all[(append_all['COUNTS_TO_MAS'] == 'YES')]
    
    append_all.geometry = gpd.points_from_xy(append_all['LONGITUDE'],append_all['LATITUDE'])
    
    
    append_all = append_all[["AGENCY",
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
        "Year_txt",
        "geometry"
    ]]
    
    # check if geometry is_valid
    # personally this is redundent, unless lat, lon is not valid, but that would throw an error in previous part
    append_all = append_all[append_all.is_valid]
    
    
    def get_entity_type(agency):
        if agency in ['CALEPA', 'CALSTA', 'CNRA', 'PARKS', 'California State Parks']:
            return 'State'
        if agency in ['DOD', 'DOI', 'USDA', 'US Department of Energy', 'NPS']:
            return 'Federal'
        if agency in ['Industrial Timber', 'Timber Companies', 'TIMBER']:
            return 'Timber Companies'
        else:
            return None
        
        
    append_all['ENTITY_TYPE'] = append_all['AGENCY'].apply(get_entity_type)
    
    return append_all


if __name__ == "__main__":
    
    append_path = r"D:\WORK\wildfire\Interagency-Tracking-System\its\ITSGDB_backup\V2.0\appended.gdb"
    report_path = r'D:\WORK\wildfire\Interagency-Tracking-System\its\ITSGDB_backup\V2.0\reports.gdb'
    report_layer_name = f'activity_report{datetime.today().strftime('%Y%m%d')}'
    
    
    enriched_polygons = gpd.read_file(append_path, driver='OpenFileGDB', layer='appended_poly')
    enriched_points = gpd.read_file(append_path, driver='OpenFileGDB', layer='appended_point')
    enriched_lines = gpd.read_file(append_path, driver='OpenFileGDB', layer='appended_line')
    
    activity_report_gdf = get_activity_report(enriched_points, enriched_lines, enriched_polygons)
    save_gdf_to_gdb(activity_report_gdf, 
                report_path, 
                report_layer_name)
    
    