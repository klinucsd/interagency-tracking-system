
import yaml
import logging
from datetime import datetime
from typing import Dict, Any
from its_logging.logger_config import logger
from utils.its_utils import layer_exists

from enrich.enrich_BLM import enrich_BLM
from enrich.enrich_NPS import enrich_NPS_from_gdb
from enrich.enrich_Timber_Industry import enrich_Timber_Industry
from enrich.enrich_Timber_Nonspatial import enrich_Timber_Nonspatial
from enrich.enrich_USFS import enrich_USFS
from enrich.enrich_NFPORS import enrich_NFPORS


logger = logging.getLogger('process.ITSProcessor')


class ITSProcessor:
    def __init__(self, config_path: str):
        with open(config_path, 'r') as f:
            self.config = yaml.safe_load(f)
        if "date" in self.config['global'].keys():
            self.date = self.config['global']['date']
        else:
            self.date = datetime.today().strftime('%Y%m%d')

    def enrich_all_except_nfpors(self):
        for source_name, source_config in self.config['sources'].items():
            if source_name == 'pfirs':
                continue
            self.enrich_source(source_name, source_config)

    def enrich_source(self, source_name: str, source_config: Dict[str, Any]):
        global_config = self.config['global']
        
        if source_name == 'blm':
            logger.info('='*80)
            logger.info('BLM Data Enrichment')

            output_gdb_path = source_config['output']['gdb_path'].format(start_year=global_config['start_year'], end_year=global_config['end_year'])
            output_layer_name = source_config['output']['layer_name'].format(date=self.date)
            if not global_config['overwrite'] and layer_exists(output_gdb_path, output_layer_name):
                logger.info(f"The layer {output_layer_name} exists in {output_gdb_path}.")
                return
                
            enrich_BLM(
                source_config['input']['gdb_path'],
                source_config['input']['layer_name'],
                global_config['reference_gdb'],
                global_config['start_year'],
                global_config['end_year'],
                output_gdb_path,
                output_layer_name
            )

        if source_name == 'nps':
            logger.info('='*80)
            logger.info('NPS Data Enrichment')
            
            output_gdb_path = source_config['output']['gdb_path'].format(start_year=global_config['start_year'], end_year=global_config['end_year'])
            output_layer_name = source_config['output']['layer_name'].format(date=self.date)
            if not global_config['overwrite'] and layer_exists(output_gdb_path, output_layer_name):
                logger.info(f"The layer {output_layer_name} exists in {output_gdb_path}.")
                return
            
            enrich_NPS_from_gdb(
                source_config['input']['gdb_path'],
                source_config['input']['layer_name'],
                global_config['reference_gdb'],
                global_config['start_year'],
                global_config['end_year'],
                output_gdb_path,
                output_layer_name
            )

        if source_name == 'timber_industry_spatial':
            logger.info('='*80)
            logger.info('Timber Industry Spatial Data Enrichment')

            output_gdb_path = source_config['output']['gdb_path'].format(start_year=global_config['start_year'], end_year=global_config['end_year'])
            output_layer_name = source_config['output']['layer_name'].format(date=self.date)
            if not global_config['overwrite'] and layer_exists(output_gdb_path, output_layer_name):
                logger.info(f"The layer {output_layer_name} exists in {output_gdb_path}.")
                return
            
            enrich_Timber_Industry(
                source_config['input']['gdb_path'],
                source_config['input']['layer_name'],
                global_config['reference_gdb'],
                global_config['start_year'],
                global_config['end_year'],
                output_gdb_path,
                output_layer_name
            )            

        if source_name == 'timber_industry_nonspatial':
            logger.info('='*80)
            logger.info('Timber Industry Non-Spatial Data Enrichment')

            output_gdb_path = source_config['output']['gdb_path'].format(start_year=global_config['start_year'], end_year=global_config['end_year'])
            output_layer_name = source_config['output']['layer_name'].format(date=self.date)
            if not global_config['overwrite'] and layer_exists(output_gdb_path, output_layer_name):
                logger.info(f"The layer {output_layer_name} exists in {output_gdb_path}.")
                return
            
            enrich_Timber_Nonspatial(
                source_config['input']['excel_path'],
                global_config['reference_gdb'],
                global_config['start_year'],
                global_config['end_year'],
                output_gdb_path,
                output_layer_name
            )            

        if source_name == 'usfs':
            logger.info('='*80)
            logger.info('USFS Data Enrichment')
            for region in source_config['input']['regions']:
                logger.info('-'*80)
                logger.info(f'USFS Data Enrichment: Region {region}')

                output_gdb_path = source_config['output']['gdb_path'].format(start_year=global_config['start_year'], end_year=global_config['end_year'])
                output_layer_name = source_config['output']['layer_name'].format(date=self.date, region=region)
                if not global_config['overwrite'] and layer_exists(output_gdb_path, output_layer_name):
                    logger.info(f"The layer {output_layer_name} exists in {output_gdb_path}.")
                    continue

                enrich_USFS(
                    f"{source_config['input']['base_path']}/{source_config['input']['gdb_template'].format(region=region)}",
                    source_config['input']['layer_name'],
                    global_config['reference_gdb'],
                    global_config['start_year'],
                    global_config['end_year'],
                    output_gdb_path,
                    output_layer_name
                )            

        if source_name == 'nfpors':
            logger.info('='*80)
            logger.info('NFPORS Data Enrichment')

            output_gdb_path = source_config['output']['gdb_path'].format(start_year=global_config['start_year'], end_year=global_config['end_year'])
            output_polygon_layer_name = f"{source_config['output']['layer_name'].format(date=self.date)}_polygons"
            output_point_layer_name = f"{source_config['output']['layer_name'].format(date=self.date)}_points"
            if not global_config['overwrite'] and layer_exists(output_gdb_path, output_polygon_layer_name) and layer_exists(output_gdb_path, output_point_layer_name):
                logger.info(f"The layer {output_polygon_layer_name} and {output_point_layer_name} exist in {output_gdb_path}.")
                return
            
            enrich_NFPORS(
                source_config['input']['gdb_path'],
                source_config['input']['polygon_layer'],
                source_config['input']['bia_layer'],
                source_config['input']['fws_layer'],
                global_config['reference_gdb'],
                global_config['start_year'],
                global_config['end_year'],
                output_gdb_path,
                source_config['output']['layer_name'].format(date=self.date)
            )            
                

if __name__ == "__main__":
    ITSProcessor('config.yaml').enrich_all_except_nfpors()
