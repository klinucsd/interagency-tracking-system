import geopandas as gpd
import sys
import yaml
sys.path.append('../')

from utils.save_gdf_to_gdb import save_gdf_to_gdb
from datetime import datetime

def get_activity_report(enriched_points, enriched_lines, enriched_polygons):
    append_all = pd.concat([enriched_lines, enriched_points, enriched_polygons])
    append_all = append_all[(append_all['COUNTS_TO_MAS'] == 'YES')]
    
    # 
    append_all.geometry = append_all.representative_point()
    
    
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
        "CORE_CRITERIA",
        "VALID_GEOM",
        "geometry"
    ]]
    
    # check if geometry is_valid
    # personally this is redundent, unless lat, lon is not valid, but that would throw an error in previous part
    append_all = append_all[append_all.is_valid]
    
    
    def get_entity_type(agency):
        if agency in ['CALEPA', 'CALSTA', 'CNRA', 'PARKS', 'California State Parks']:
            return 'State'
        if agency in ['DOD', 'DOI', 'USDA', 'DOE', 'NPS']:
            return 'Federal'
        if agency in ['Industrial Timber', 'Timber Companies', 'TIMBER']:
            return 'Timber Companies'
        else:
            return None
        
        
    append_all['ENTITY_TYPE'] = append_all['AGENCY'].apply(get_entity_type)
    
    return append_all


if __name__ == "__main__":

    with open("..\config.yaml", 'r') as stream:
        config_inputs = yaml.safe_load(stream)
    
    append_path = config_inputs['appended']['gdb_path']
    output_report_path = config_inputs['activity']['gdb_path']
    # not formally used in report generation
    # can limit activity year range for better front end display
    start_year = config_inputs['activity']['start_year']
    end_year = config_inputs['activity']['end_year']

    output_format_dict = {'date': datetime.today().strftime('%Y%m%d')}
    report_layer_name = config_inputs['activity']['report_layer_name'].format(**output_format_dict)
    
    
    enriched_polygons = gpd.read_file(append_path, driver='OpenFileGDB', layer='appended_poly')
    enriched_points = gpd.read_file(append_path, driver='OpenFileGDB', layer='appended_point')
    enriched_lines = gpd.read_file(append_path, driver='OpenFileGDB', layer='appended_line')

    
    activity_report_gdf = get_activity_report(enriched_points, enriched_lines, enriched_polygons)
    save_gdf_to_gdb(activity_report_gdf, 
                output_report_path, 
                report_layer_name)
    
    