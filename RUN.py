import psutil
import os
from datetime import datetime

from its_logging.logger_config import logger
from enrich.enrich_USFS import enrich_USFS
#from utils.mp_df import ObjProxy, MyDataFrame
from multiprocessing.managers import BaseManager
from multiprocessing import Manager

a_reference_gdb_path = r"D:\WORK\wildfire\Interagency-Tracking-System\its\Interagency Tracking System.gdb"
start_year, end_year = 2020, 2025



# Create a class during runtime
#new_dict = ObjProxy.populate_obj_attributes(MyDataFrame)
#DataFrameObjProxy = type("DataFrameObjProxy", (ObjProxy,), new_dict)


if __name__ == "__main__":
    process = psutil.Process(os.getpid())
    #BaseManager.register('DataFrame', MyDataFrame, DataFrameObjProxy, exposed=tuple(dir(DataFrameObjProxy)))
    output_gdb_path = r"D:\WORK\wildfire\Interagency-Tracking-System\its\ITSGDB_backup\tmp\USFS_{}_{}.gdb".format(start_year, end_year)
    region_ids = ["05"]#["04", "05", "06"]
    for region_id in region_ids:    
        usfs_input_gdb_path = r'D:\WORK\wildfire\Interagency-Tracking-System\2023\USFS_FACTS_2023\USFS_FACTS_2023_20240620_uploadEmilyBrodie\Actv_CommonAttribute_PL_Region{}.gdb'.format(region_id)
        usfs_input_layer_name = "Actv_CommonAttribute_PL"
        output_layer_name = f"USFS_Region{region_id}_enriched_{datetime.today().strftime('%Y%m%d')}"
        # init multiprocessing manager in main module for Windows fork
        #manager = BaseManager()
        #manager.start()

        manager = Manager()
        
        enrich_USFS(usfs_input_gdb_path,
                    usfs_input_layer_name,
                    a_reference_gdb_path,
                    start_year,
                    end_year,
                    output_gdb_path,
                    output_layer_name,
                    manager=manager)


