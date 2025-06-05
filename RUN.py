import psutil
import os
from datetime import datetime

from its_logging.logger_config import logger
from enrich.enrich_USFS import enrich_USFS
#from utils.mp_df import ObjProxy, MyDataFrame
from multiprocessing.managers import BaseManager
from multiprocessing import Manager

a_reference_gdb_path = r"D:\WORK\wildfire\Interagency-Tracking-System\its\Interagency Tracking System.gdb"
start_year, end_year = 1950, 2025



# Create a class during runtime
#new_dict = ObjProxy.populate_obj_attributes(MyDataFrame)
#DataFrameObjProxy = type("DataFrameObjProxy", (ObjProxy,), new_dict)


if __name__ == "__main__":
    process = psutil.Process(os.getpid())

    from enrich.enrich_NFPORS_copy import enrich_NFPORS

    nfpors_polygon_layer = r"D:\WORK\wildfire\Interagency-Tracking-System\V2.0\NFPORS_V2.0\NFPORS_V2.0_FTP_shp\NFPORS_V2.0_FTP.shp"
    nfpors_bia_fws_layer = r"D:\WORK\wildfire\Interagency-Tracking-System\V2.0\NFPORS_V2.0\NFPORS_V2.0_BIA_FWS_shp\\NFPORS_V2.0_BIA_FWS.shp"
    output_gdb_path = r"D:\WORK\wildfire\Interagency-Tracking-System\its\ITSGDB_backup\V2.0\NFPORS_{}_{}.gdb".format(start_year, end_year)
    output_layer_name = f"NFPORS_enriched_{datetime.today().strftime('%Y%m%d')}"
    enrich_NFPORS(nfpors_polygon_layer,
                  nfpors_bia_fws_layer,
                  a_reference_gdb_path,
                  start_year,
                  end_year,
                  output_gdb_path,
                  output_layer_name)