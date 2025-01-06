
import os
import time
import math
import json
import logging
import geopandas as gpd
import pandas as pd
import numpy as np
from multiprocessing import Pool
from typing import Tuple, List
from shapely.geometry import Point, LineString, Polygon
from shapely.ops import polygonize

from its_logging.logger_config import logger
from utils.its_utils import get_wfr_tf_template
from utils.gdf_utils import get_rows_with_empty_geometry

logger = logging.getLogger('process.footprint')


def feature_to_polygon(input_gdf):
    # Get all line geometries
    lines = input_gdf.geometry.unary_union
    
    # Convert lines to polygons
    polygons = list(polygonize(lines))
    
    # Create new GeoDataFrame with the polygons
    polygon_gdf = gpd.GeoDataFrame(geometry=polygons, crs=input_gdf.crs)
    
    return polygon_gdf


def process_chunk(args):
    spaghetti, chunk = args
    return gpd.overlay(spaghetti, chunk, how='identity')


def update_pt(enriched_points: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    """Update points with buffer calculations."""
    logger.info(f"      initial points count: {len(enriched_points)}")
    
    # Add BufferMeters field
    enriched_points['BufferMeters'] = None
    
    # Calculate buffer for AC units
    mask1 = (enriched_points['ACTIVITY_QUANTITY'].notna()) & (enriched_points['ACTIVITY_UOM'] == 'AC')
    logger.info(f"      points with valid ACTIVITY_QUANTITY and AC units: {mask1.sum()}")
    
    enriched_points.loc[mask1, 'BufferMeters'] = np.sqrt((enriched_points.loc[mask1, 'ACTIVITY_QUANTITY'] * 4046.86) / math.pi)
    
    # Filter for COUNTS_TO_MAS
    mask2 = (enriched_points['COUNTS_TO_MAS'] == 'YES') & (enriched_points['BufferMeters'].notna())
    logger.info(f"      points with COUNTS_TO_MAS='YES' and valid BufferMeters: {mask2.sum()}")
    
    selected_points = enriched_points[mask2].copy()

    selected_points = selected_points[selected_points['BufferMeters'] > 0]
    logger.info(f"      points with BufferMeters > 0: {len(selected_points)}")
    
    # Create buffers
    buffered_geoms = selected_points.apply(
        lambda row: row.geometry.buffer(row['BufferMeters']) if pd.notnull(row['BufferMeters']) else row.geometry,
        axis=1
    )
    
    # Create new GeoDataFrame with buffered geometries
    result = selected_points.copy()
    result.geometry = buffered_geoms
    logger.info(f"      final points count: {len(result)}")
    return result


def update_ln(enriched_lines: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    """Update lines with buffer calculations."""
    logger.info(f"      initial lines count: {len(enriched_lines)}")
        
    # Check for case differences
    if 'ACTIVITY_UOM' in enriched_lines.columns:
        unique_uom = enriched_lines['ACTIVITY_UOM'].unique()
        logger.info(f"      unique ACTIVITY_UOM values: {unique_uom}")
    
    # Add BufferMeters field
    enriched_lines['BufferMeters'] = None
    
    # Calculate line lengths
    line_lengths = enriched_lines.geometry.length
    logger.info(f"      lines with length > 0: {(line_lengths > 0).sum()}")
    
    # Modify conditions to be more lenient and check for case variations
    condition1 = enriched_lines['ACTIVITY_QUANTITY'].notna() if 'ACTIVITY_QUANTITY' in enriched_lines.columns else False
    logger.info(f"      lines with valid ACTIVITY_QUANTITY: {condition1.sum() if isinstance(condition1, pd.Series) else 0}")
    
    condition2 = enriched_lines['ACTIVITY_UOM'].str.upper() == 'AC' if 'ACTIVITY_UOM' in enriched_lines.columns else False
    logger.info(f"      lines with ACTIVITY_UOM = 'AC' (case insensitive): {condition2.sum() if isinstance(condition2, pd.Series) else 0}")
    
    # Check combined conditions
    mask1 = condition1 & condition2 & (line_lengths > 0)
    logger.info(f"      lines meeting all conditions: {mask1.sum() if isinstance(mask1, pd.Series) else 0}")
    
    if not isinstance(mask1, pd.Series) or mask1.sum() == 0:
        logger.warning("      no lines meet the filtering criteria. Please check if the required columns exist and contain expected values.")
        return enriched_lines[enriched_lines['geometry'].notna()].head(0)  # Return empty GeoDataFrame with same schema
    
    # Calculate buffer distances
    enriched_lines.loc[mask1, 'BufferMeters'] = (
        (enriched_lines.loc[mask1, 'ACTIVITY_QUANTITY'] * 4046.86) / 
        line_lengths[mask1] / 2
    )
    
    # Check COUNTS_TO_MAS condition
    condition3 = enriched_lines['COUNTS_TO_MAS'] == 'YES' if 'COUNTS_TO_MAS' in enriched_lines.columns else False
    logger.info(f"      lines with COUNTS_TO_MAS = 'YES': {condition3.sum() if isinstance(condition3, pd.Series) else 0}")
    
    condition4 = ~((enriched_lines['BufferMeters'] >= 200) & (enriched_lines['Source'] == 'CalTrans'))
    
    # Final filter
    mask2 = condition3 & condition4
    selected_lines = enriched_lines[mask2].copy()
    
    # Create buffers
    buffered_geoms = selected_lines.apply(
        lambda row: row.geometry.buffer(row['BufferMeters']) if pd.notnull(row['BufferMeters']) else row.geometry,
        axis=1
    )
    
    # Create new GeoDataFrame with buffered geometries
    result = selected_lines.copy()
    result.geometry = buffered_geoms
    logger.info(f"      final lines count: {len(result)}")
    
    return result


def update_poly(enriched_polygons: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    """Update polygons with selection criteria."""
    logger.info(f"      initial polygons count: {len(enriched_polygons)}")

    mas_gdf = enriched_polygons[enriched_polygons['COUNTS_TO_MAS'] == 'YES']
    logger.info(f"      polygons with COUNTS_TO_MAS = 'YES': {mas_gdf.shape}")

    treatment_gdf = enriched_polygons[enriched_polygons['TREATMENT_AREA'] < 100000]
    logger.info(f"      polygons with 'TREATMENT_AREA' < 100000: {treatment_gdf.shape}")
                                      
    mask = (
        (enriched_polygons['COUNTS_TO_MAS'] == 'YES') & 
        (enriched_polygons['TREATMENT_AREA'] < 100000)
    )

    final_gdf = enriched_polygons[mask].copy()
    logger.info(f"      final polygons: {final_gdf.shape}")
    
    return final_gdf


def get_footprint(
        input_gdf: gpd.GeoDataFrame,
        reference_gdb_path: str,
        year_start: int,
        year_end: int
) -> Tuple[gpd.GeoDataFrame, gpd.GeoDataFrame]:
    """Generate footprint analysis for specified years."""

    footprint_lst = []
    footprint_pt_lst = []

    # load Own_Veg_Region_WUI
    logger.info(f"      Loading Own_Veg_Region_WUI...")

    start = time.time()
    if not os.path.exists("cache"):
        os.makedirs("cache")
    if os.path.exists("cache/Own_Veg_Region_WUI.parquet"):
        own_veg_region_wui = gpd.read_parquet("cache/Own_Veg_Region_WUI.parquet")
        logger.info("         Loaded Own_Veg_Region_WUI from cache")
    else:
        own_veg_region_wui = gpd.read_file(reference_gdb_path, driver="OpenFileGDB", sql_dialect="OGRSQL", sql=f"SELECT *, OBJECTID FROM Own_Veg_Region_WUI")
        own_veg_region_wui.to_parquet("cache/Own_Veg_Region_WUI.parquet")
        logger.info("         loaded Own_Veg_Region_WUI from source and cached")
    logger.info(f"         time for loading Own_Veg_Region_WUI: {time.time()-start}")
    logger.info(f"         loaded {own_veg_region_wui.shape[0]} records")
    
    for year in range(year_start, year_end + 1):
        logger.info(f"      Processing year: {year}")
        
        # Filter by year
        year_mask = input_gdf['Year'] == year
        year_data = input_gdf[year_mask].copy()
        
        logger.info(f"         Year {year} has {len(year_data)} records")
        if len(year_data) == 0:
            continue
        
        # Create points (meatballs)
        meatballs = year_data.copy()
        meatballs["geometry"] = meatballs.geometry.representative_point()
        # meatballs['geometry'] = meatballs.geometry.centroid
        logger.info(f"            meatballs: {meatballs.shape[0]} records")

        # Dissolve features by TRMTID_USER (equivalent to PairwiseDissolve)
        spaghetti = year_data.dissolve(by='TRMTID_USER', as_index=False)
    
        # Convert multipart to singlepart (equivalent to FeatureToPolygon)
        spaghetti_polygons = spaghetti.explode(index_parts=True)
        spaghetti_polygons = spaghetti_polygons.reset_index(drop=True)
        logger.info(f"            spaghetti_polygons: {len(spaghetti_polygons)} records")
            
        # Drop specified fields
        spaghetti_polygons = spaghetti_polygons.drop(columns=['TRMTID_USER'])
    
        # Perform spatial join (equivalent to Identity)
        # Using spatial index for better performance
        logger.info(f"         Making spaghetti_sauce ...")
        spaghetti_sauce = gpd.overlay(spaghetti_polygons, own_veg_region_wui, how='identity')
        spaghetti_sauce = spaghetti_sauce.set_crs('EPSG:3310', allow_override=True)
        logger.info(f"            spaghetti_sauce: {spaghetti_sauce.shape[0]} records")
        
        # Update CalTrans ownership
        caltrans_mask = input_gdf['AGENCY'] == 'CALSTA'
        caltrans_projects = input_gdf[caltrans_mask].dissolve(by='AGENCY')
        
        # Spatial join for CalTrans projects
        caltrans_join = gpd.sjoin(
            spaghetti_sauce,
            caltrans_projects,
            how='left',
            predicate='within'
        )
        caltrans_join.loc[caltrans_join['index_right'].notna(), 'PRIMARY_OWNERSHIP_GROUP'] = 'STATE'
        
        # Calculate area
        caltrans_join['FootprintAcres'] = caltrans_join.geometry.area * 0.000247105  # Convert sq meters to acres
        
        # Summarize with points
        dinner = gpd.sjoin(caltrans_join, meatballs)
        dinner = dinner.groupby(dinner.index).agg({
            'ACTIVITY_QUANTITY': ['mean', 'max'],
            'FootprintAcres': 'first',
            'geometry': 'first'
        }).reset_index()
        
        dinner['Year_txt'] = str(year)
        
        # Create points version
        dinner_pts = dinner.copy()
        dinner_pts['geometry'] = dinner_pts.geometry.centroid
        
        footprint_lst.append(dinner)
        footprint_pt_lst.append(dinner_pts)
    
    # Combine all years
    footprint_out = pd.concat(footprint_lst, ignore_index=True)
    footprint_pt_out = pd.concat(footprint_pt_lst, ignore_index=True)
    
    return gpd.GeoDataFrame(footprint_out), gpd.GeoDataFrame(footprint_pt_out)


def get_footprint_report(
        enriched_polygons: gpd.GeoDataFrame,
        enriched_lines: gpd.GeoDataFrame,
        enriched_points: gpd.GeoDataFrame,
        start_year: int,
        end_year: int,
        reference_gdb_path: str,
        output_footprint_gdb: str,
        output_footprint_name: str,
        output_footprint_pts_name: str
) -> Tuple[gpd.GeoDataFrame, gpd.GeoDataFrame]:

    logger.info("-"*80)
    logger.info("Generate Footprint Report...")

    if get_rows_with_empty_geometry(enriched_points)[0] > 0:
        logger.error("Found empty geometry in enriched_points: ")
        exit()

    if get_rows_with_empty_geometry(enriched_lines)[0] > 0:
        logger.error("Found empty geometry in enriched_lines: ")
        exit()

    if get_rows_with_empty_geometry(enriched_polygons)[0] > 0:
        logger.error("Found empty geometry in enriched_polygons: ")
        exit()
        
    logger.info("   Processing points...")
    buffer_pts = update_pt(enriched_points)

    if get_rows_with_empty_geometry(buffer_pts)[0] > 0:
        logger.error("Found empty geometry in buffer_pts: ")
        exit()

    logger.info("   Processing lines...")
    buffer_lines = update_ln(enriched_lines)

    if get_rows_with_empty_geometry(buffer_lines)[0] > 0:
        logger.error("Found empty geometry in buffer_lines")
        exit()
    
    logger.info("   Processing polygons...")
    processed_polys = update_poly(enriched_polygons)

    if get_rows_with_empty_geometry(processed_polys)[0] > 0:
        logger.error("Found empty geometry in processed_polys")
        exit()
    
    # Combine all features
    combined_features = pd.concat([
        buffer_pts,
        buffer_lines,
        processed_polys
    ], ignore_index=True)
    
    template_gdf = get_wfr_tf_template(reference_gdb_path)
    template_gdf = template_gdf.drop(columns=['BatchID_p', 'BatchID', 'Shape_Length', 'Shape_Area'])
    combined_features = combined_features[template_gdf.columns]
    
    logger.info("   Generating footprints...")
    footprint_poly, footprint_pts = get_footprint(
        combined_features,
        reference_gdb_path,
        start_year,
        end_year
    )
    
    # Save outputs if paths provided
    if output_footprint:
        # footprint_poly.to_file(output_footprint)
        pass
    if output_footprint_pts:
        # footprint_pts.to_file(output_footprint_pts)
        pass
    
    return footprint_poly, footprint_pts
