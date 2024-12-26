
import geopandas as gpd
from osgeo import ogr


def clip_to_california(gdf, ca_gdb_path):
    california = gpd.read_file(ca_gdb_path, driver="OpenFileGDB", layer='California')
    return gdf.clip(california)

def get_wfr_tf_template(ca_gdb_path):
    return gpd.read_file(ca_gdb_path, driver="OpenFileGDB", layer='WFR_TF_Template')

def layer_exists(gdb_path, layer_name):
    driver = ogr.GetDriverByName("OpenFileGDB")
    gdb = driver.Open(gdb_path, 0)
    if gdb is None:
        return False
    layer = gdb.GetLayerByName(layer_name)
    gdb = None  # Close the connection
    return layer is not None
