
import geopandas as gpd


def clip_to_california(gdf, ca_gdb_path):
    california = gpd.read_file(ca_gdb_path, driver="OpenFileGDB", layer='California')
    return gdf.clip(california)

def get_wfr_tf_template(ca_gdb_path):
    return gpd.read_file(ca_gdb_path, driver="OpenFileGDB", layer='WFR_TF_Template')
