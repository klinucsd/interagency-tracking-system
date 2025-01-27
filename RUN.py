import psutil
import os
from datetime import datetime

from its_logging.logger_config import logger
from enrich.enrich_CNRA import enrich_CNRA
from enrich.enrich_BLM import enrich_BLM
from enrich.enrich_NPS import enrich_NPS_from_gdb

a_reference_gdb_path = r"D:\WORK\wildfire\Interagency-Tracking-System\its\Interagency Tracking System.gdb"
start_year, end_year = 2010, 2025

if __name__ == "__main__":
    process = psutil.Process(os.getpid())
    """
    cnra_input_gdb_path = r"D:\WORK\wildfire\Interagency-Tracking-System\2023\CNRA_2023\CNRA_Tracker_Data_Export_20241120.gdb"
    cnra_polygon_layer_name = "TREATMENT_POLY"
    cnra_line_layer_name = "TREATMENT_LINE"
    cnra_point_layer_name = "TREATMENT_POINT"
    cnra_project_polygon_layer_name = "PROJECT_POLY"
    cnra_activity_layer_name = "ACTIVITIES"
    output_gdb_path = r"D:\WORK\wildfire\Interagency-Tracking-System\its\ITSGDB_backup\tmp\CNRA_{}_{}.gdb".format(start_year, end_year)
    output_layer_name = f"CNRA_enriched_{datetime.today().strftime('%Y%m%d')}"


    
    enrich_CNRA(cnra_input_gdb_path,
                cnra_polygon_layer_name,
                cnra_line_layer_name,
                cnra_point_layer_name,
                cnra_project_polygon_layer_name,
                cnra_activity_layer_name,
                a_reference_gdb_path,
                start_year,
                end_year,
                output_gdb_path,
                output_layer_name)
    
    blm_input_gdb_path = r"D:\WORK\wildfire\Interagency-Tracking-System\2023\BLM_2023\BLM_2010_2023_fromReisThomasViaUpload.gdb"
    blm_input_layer_name = "BLM_2010_2023_fromReisThomasViaUpload"
    output_gdb_path = r"D:\WORK\wildfire\Interagency-Tracking-System\its\ITSGDB_backup\tmp\BLM_{}_{}.gdb".format(start_year, end_year)
    output_layer_name = f"BLM_enriched_{datetime.today().strftime('%Y%m%d')}"
    
    enrich_BLM(blm_input_gdb_path,
               blm_input_layer_name,
               a_reference_gdb_path,
               start_year,
               end_year,
               output_gdb_path,
               output_layer_name)
    """


    nps_gdb_path = r'D:\WORK\wildfire\Interagency-Tracking-System\2023\NPS_2023\New_NPS_2023_20240625_ReisThomasViaUpload_1.gdb'
    nps_layer_name = 'NPS_2023_20240625_ReisThomasViaUpload2'
    output_gdb_path =  r"D:\WORK\wildfire\Interagency-Tracking-System\its\ITSGDB_backup\tmp\NPS_{}_{}.gdb".format(start_year, end_year)
    output_layer_name = f"NPS_enriched_{datetime.today().strftime('%Y%m%d')}"

    enrich_NPS_from_gdb(nps_gdb_path,
                        nps_layer_name,
                        a_reference_gdb_path,
                        start_year,
                        end_year,
                        output_gdb_path,
                        output_layer_name)

    # Get memory usage in bytes, convert to MB
    memory_usage = process.memory_info().rss / 1024 / 1024
    logger.info(f"Memory usage: {memory_usage:.2f} MB")
