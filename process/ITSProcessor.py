
import os
import json
import yaml
import logging
import psutil

from datetime import datetime
from typing import Dict, List, Any
from its_logging.logger_config import logger
from utils.its_utils import layer_exists

from enrich.enrich_BLM import enrich_BLM
from enrich.enrich_NPS import enrich_NPS_from_gdb
from enrich.enrich_Timber_Industry import enrich_Timber_Industry
from enrich.enrich_Timber_Nonspatial import enrich_Timber_Nonspatial
from enrich.enrich_USFS import enrich_USFS
from enrich.enrich_NFPORS import enrich_NFPORS
from enrich.enrich_CNRA import enrich_CNRA
from enrich.enrich_PFIRS import enrich_PFIRS
from enrich.enrich_CalTrans import enrich_Caltrans

from process.append import append_enriched_features, get_enriched_features
from process.transform import transform_projects, transform_treatments, transform_activities
from process.footprint_report import get_footprint_report


logger = logging.getLogger('process.ITSProcessor')


class ITSProcessor:

    def __init__(self, config_path: str):
        with open(config_path, 'r') as f:
            self.config = yaml.safe_load(f)
        if "date" in self.config['global'].keys():
            self.date = self.config['global']['date']
        else:
            self.date = datetime.today().strftime('%Y%m%d')

    def enrich_all_except_pfirs(self):
        enriched_layers = {
            'point': [],
            'line': [],
            'polygon': []
        }
        for source_name, source_config in self.config['sources'].items():
            if source_name == 'pfirs':
                continue
            self.enrich_source(source_name, source_config, enriched_layers)
        return enriched_layers

    
    def enrich_source(self, source_name: str, source_config: Dict[str, Any], enriched_layers: List):
        global_config = self.config['global']

        # Enrich BLM data
        if source_name == 'blm':
            logger.info('='*80)
            logger.info('BLM Data Enrichment')

            output_gdb_path = source_config['output']['gdb_path'].format(start_year=global_config['start_year'], end_year=global_config['end_year'])
            output_layer_name = source_config['output']['layer_name'].format(date=self.date)
            enriched_layers['polygon'].append({
                'gdb_path': output_gdb_path,
                'layer_name': output_layer_name
            })
            
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

        # Enrich NPS data
        if source_name == 'nps':
            logger.info('='*80)
            logger.info('NPS Data Enrichment')
            
            output_gdb_path = source_config['output']['gdb_path'].format(start_year=global_config['start_year'], end_year=global_config['end_year'])
            output_layer_name = source_config['output']['layer_name'].format(date=self.date)
            enriched_layers['polygon'].append({
                'gdb_path': output_gdb_path,
                'layer_name': output_layer_name
            })
            
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

        # Enrich Timber industry spatial data
        if source_name == 'timber_industry_spatial':
            logger.info('='*80)
            logger.info('Timber Industry Spatial Data Enrichment')

            output_gdb_path = source_config['output']['gdb_path'].format(start_year=global_config['start_year'], end_year=global_config['end_year'])
            output_layer_name = source_config['output']['layer_name'].format(date=self.date)
            enriched_layers['polygon'].append({
                'gdb_path': output_gdb_path,
                'layer_name': output_layer_name
            })
            
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

        # Enrich Timber industry nonspatial data
        if source_name == 'timber_industry_nonspatial':
            logger.info('='*80)
            logger.info('Timber Industry Non-Spatial Data Enrichment')

            output_gdb_path = source_config['output']['gdb_path'].format(start_year=global_config['start_year'], end_year=global_config['end_year'])
            output_layer_name = source_config['output']['layer_name'].format(date=self.date)
            enriched_layers['point'].append({
                'gdb_path': output_gdb_path,
                'layer_name': output_layer_name
            })

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

        # Enrich USPS data
        if source_name == 'usfs':
            logger.info('='*80)
            logger.info('USFS Data Enrichment')
            for region in source_config['input']['regions']:
                logger.info('-'*80)
                logger.info(f'USFS Data Enrichment: Region {region}')

                output_gdb_path = source_config['output']['gdb_path'].format(start_year=global_config['start_year'], end_year=global_config['end_year'])
                output_layer_name = source_config['output']['layer_name'].format(date=self.date, region=region)
                enriched_layers['polygon'].append({
                    'gdb_path': output_gdb_path,
                    'layer_name': output_layer_name
                })

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

        # Enrich NFPORS data
        if source_name == 'nfpors':
            logger.info('='*80)
            logger.info('NFPORS Data Enrichment')

            output_gdb_path = source_config['output']['gdb_path'].format(start_year=global_config['start_year'], end_year=global_config['end_year'])
            output_polygon_layer_name = f"{source_config['output']['layer_name'].format(date=self.date)}_polygon"
            output_point_layer_name = f"{source_config['output']['layer_name'].format(date=self.date)}_point"
            enriched_layers['polygon'].append({
                    'gdb_path': output_gdb_path,
                    'layer_name': output_polygon_layer_name
                })
            enriched_layers['point'].append({
                    'gdb_path': output_gdb_path,
                    'layer_name': output_point_layer_name
                })
            
            if not global_config['overwrite'] and \
               layer_exists(output_gdb_path, output_polygon_layer_name) and \
               layer_exists(output_gdb_path, output_point_layer_name):
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

        # Enrich CNRA data
        if source_name == 'cnra':
            logger.info('='*80)
            logger.info('CNRA Data Enrichment')
            
            output_gdb_path = source_config['output']['gdb_path'].format(start_year=global_config['start_year'], end_year=global_config['end_year'])
            output_layer_name = source_config['output']['layer_name'].format(date=self.date)
            output_polygon_layer_name = f"{output_layer_name}_polygon"
            output_line_layer_name = f"{output_layer_name}_line"
            output_point_layer_name = f"{output_layer_name}_point"

            enriched_layers['polygon'].append({
                    'gdb_path': output_gdb_path,
                    'layer_name': output_polygon_layer_name
                })
            enriched_layers['line'].append({
                    'gdb_path': output_gdb_path,
                    'layer_name': output_line_layer_name
                })
            enriched_layers['point'].append({
                    'gdb_path': output_gdb_path,
                    'layer_name': output_point_layer_name
                })
            
            if not global_config['overwrite'] and \
               layer_exists(output_gdb_path, output_polygon_layer_name) and \
               layer_exists(output_gdb_path, output_line_layer_name) and \
               layer_exists(output_gdb_path, output_point_layer_name):
                logger.info(f"The layer {output_layer_name} exists in {output_gdb_path}.")
                return
            
            enrich_CNRA(
                source_config['input']['gdb_path'],
                source_config['input']['polygon_layer_name'],
                source_config['input']['line_layer_name'],
                source_config['input']['point_layer_name'],
                source_config['input']['project_layer_name'],
                source_config['input']['activity_layer_name'],
                global_config['reference_gdb'],
                global_config['start_year'],
                global_config['end_year'],
                output_gdb_path,
                output_layer_name
            )

        # Enrich Caltrans data
        if source_name == 'caltrans':
            logger.info('='*80)
            logger.info('CALTRANS Data Enrichment')

            output_gdb_path = source_config['output']['gdb_path'].format(start_year=global_config['start_year'], end_year=global_config['end_year'])
            output_layer_name = source_config['output']['layer_name'].format(date=self.date)
            enriched_layers['line'].append({
                'gdb_path': output_gdb_path,
                'layer_name': output_layer_name
            })
            
            if not global_config['overwrite'] and layer_exists(output_gdb_path, output_layer_name):
                logger.info(f"The layer {output_layer_name} exists in {output_gdb_path}.")
                return
                
            enrich_Caltrans(
                source_config['input']['base_path'],
                None, None,
                source_config['input']['road_activity_layer_name'],
                source_config['input']['road_treatment_layer_name'],
                global_config['reference_gdb'],
                global_config['start_year'],
                global_config['end_year'],
                output_gdb_path,
                output_layer_name
            )


            
    def enrich_PFIRS(self, enriched_polygons):

        logger.info('='*80)
        logger.info('PFIRS Data Enrichment')

        global_config = self.config['global']        
        source_config = self.config['sources']['pfirs']

        output_gdb_path = source_config['output']['gdb_path'].format(start_year=global_config['start_year'], end_year=global_config['end_year'])
        output_layer_name = source_config['output']['layer_name'].format(date=self.date)
        enriched_layers['point'].append({
            'gdb_path': output_gdb_path,
            'layer_name': output_layer_name
        })
            
        if not global_config['overwrite'] and layer_exists(output_gdb_path, output_layer_name):
            logger.info(f"The layer {output_layer_name} exists in {output_gdb_path}.")
            return
        
        enrich_PFIRS(source_config['input']['gdb_path'],
                     source_config['input']['layer_name'],
                     enriched_polygons,
                     global_config['reference_gdb'],
                     global_config['start_year'],
                     global_config['end_year'],
                     output_gdb_path,
                     output_layer_name)


    def transform(self, enriched_layers):
        # Concatenate enriched points, lines and polygons
        enriched_polygons, enriched_lines, enriched_points = get_enriched_features(enriched_layers)

        logger.info("-"*80)
        transform_projects(enriched_polygons, enriched_lines, enriched_points)

        logger.info("-"*80)
        transform_treatments(enriched_polygons, enriched_lines, enriched_points)

        logger.info("-"*80)
        transform_activities(enriched_polygons, enriched_lines, enriched_points)
        

    def create_footprint_report(self, enriched_layers):
        enriched_polygons, enriched_lines, enriched_points = get_enriched_features(enriched_layers)
        get_footprint_report(enriched_polygons,
                             enriched_lines,
                             enriched_points,
                             self.config['global']['start_year'],
                             self.config['global']['end_year'],
                             self.config['global']['reference_gdb'],
                             self.config['footprint']['gdb_path'],
                             self.config['footprint']['report_layer_name'],
                             self.config['footprint']['point_layer_name'])

        
if __name__ == "__main__":
    
    # Get the current process ID
    process = psutil.Process(os.getpid())
    
    # Create a batch processor
    its_processor = ITSProcessor('config.yaml')

    # Enrich data for all agencies except PFIRS
    enriched_layers = its_processor.enrich_all_except_pfirs()

    # Concatenate enriched polygons
    logger.info("="*80)
    logger.info("Preparing enriched polygon data for PFIRS...")
    enriched_polygons = append_enriched_features(enriched_layers['polygon'])

    # Enrich PFIRS
    its_processor.enrich_PFIRS(enriched_polygons)

    # Show enriched data summary
    logger.info('='*80)
    logger.info(f"Enriched Result Summary")
    for layer_type in enriched_layers.keys():
        logger.info(f"   {layer_type.title()}")
        for layer in enriched_layers[layer_type]:
            logger.info(f"      {layer['layer_name']} in {layer['gdb_path']}")

    # Transform enriched data
    # its_processor.transform(enriched_layers)

    its_processor.create_footprint_report(enriched_layers)

    
    # Get memory usage in bytes, convert to MB
    memory_usage = process.memory_info().rss / 1024 / 1024
    logger.info(f"Memory usage: {memory_usage:.2f} MB")
