
import os
import uuid
import logging

import geopandas as gpd
import pandas as pd
import numpy as np

from shapely.geometry import (
    MultiPolygon, 
    MultiPoint, 
    MultiLineString, 
    Polygon, 
    Point, 
    LineString,
    base
)
from shapely.geometry import Polygon, MultiPolygon
from its_logging.logger_config import logger


logger = logging.getLogger('process.transform')


def safe_buffer(geometry, distance):
    """
    Safely buffer a geometry with validation of buffer distance.
    
    Parameters:
    -----------
    geometry : shapely.geometry
        Geometry to buffer
    distance : float
        Buffer distance in meters
        
    Returns:
    --------
    shapely.geometry
        Buffered geometry or original geometry if buffer distance is invalid
    """
    if pd.isna(distance) or np.isinf(distance) or distance <= 0:
        return geometry
    return geometry.buffer(distance)


def transform_projects(enriched_polygons, enriched_lines, enriched_points):

    logger = logging.getLogger('process.tran_projects')
    
    logger.info("Transform Projects ...")
    
    # Define potential dissolve fields
    all_dissolve_fields = [
        "PROJECTID_USER", "AGENCY", "ORG_ADMIN_p", "PROJECT_CONTACT",
        "PROJECT_EMAIL", "ADMINISTERING_ORG", "PROJECT_NAME",
        "PROJECT_STATUS", "PROJECT_START", "PROJECT_END",
        "PRIMARY_FUNDING_SOURCE", "PRIMARY_FUNDING_ORG",
        "IMPLEMENTING_ORG", "BatchID_p", "Val_Status_p",
        "Val_Msg_p", "Val_RunDate_p", "Review_Status_p",
        "Review_Msg_p", "Review_RunDate_p", "Dataload_Status_p",
        "Dataload_Msg_p"
    ]
    
    logger.info("   Start buffer selection")
    
    # Process points
    valid_points = enriched_points[
        enriched_points['ACTIVITY_QUANTITY'].notna() & 
        (enriched_points['ACTIVITY_QUANTITY'] > 0)
    ].copy()
    
    valid_points['BufferMeters'] = np.sqrt(
        (valid_points['ACTIVITY_QUANTITY'] * 4046.86) / np.pi
    )
    
    logger.info("   Start buffering points")
    buffered_points = valid_points.copy()
    buffered_points['geometry'] = buffered_points.apply(
        lambda row: safe_buffer(row.geometry, row['BufferMeters']),
        axis=1
    )
    
    # Process lines
    logger.info("   Start line selection")
    valid_lines = enriched_lines[
        enriched_lines.geometry.length.notna() & 
        (enriched_lines.geometry.length > 0) &
        enriched_lines['ACTIVITY_QUANTITY'].notna() &
        (enriched_lines['ACTIVITY_QUANTITY'] > 0)
    ].copy()
    
    valid_lines['BufferMeters'] = (
        (valid_lines['ACTIVITY_QUANTITY'] * 4046.86) / 
        valid_lines.geometry.length / 2
    )
    
    logger.info("   Start buffering lines")
    buffered_lines = valid_lines.copy()
    buffered_lines['geometry'] = buffered_lines.apply(
        lambda row: safe_buffer(row.geometry, row['BufferMeters']),
        axis=1
    )
    
    # Add BufferMeters field to polygons
    enriched_polygons['BufferMeters'] = None
    
    logger.info("   Combining features")
    combined_features = pd.concat([
        enriched_polygons,
        buffered_points,
        buffered_lines
    ], ignore_index=True)
    
    # Get the list of fields that actually exist in the combined features
    available_dissolve_fields = [field for field in all_dissolve_fields 
                               if field in combined_features.columns]
    
    logger.info(f"   Using dissolve fields: {available_dissolve_fields}")
    
    logger.info("   Start dissolving by features")
    # Create unique identifier using only available fields
    if available_dissolve_fields:
        combined_features['TEMP_UID'] = combined_features[available_dissolve_fields].apply(
            lambda x: '_'.join(x.astype(str).fillna('NA')),
            axis=1
        )
    else:
        # If no dissolve fields are available, create a unique ID for each feature
        logger.warning("   No dissolve fields available, creating unique IDs")
        combined_features['TEMP_UID'] = range(len(combined_features))
    
    # Dissolve features
    dissolved = combined_features.dissolve(by='TEMP_UID').reset_index()
    
    # Ensure all geometries are MultiPolygons
    dissolved['geometry'] = dissolved['geometry'].apply(
        lambda geom: MultiPolygon([geom]) if isinstance(geom, Polygon) else geom
    )
    
    # Convert fields to string type
    for field in available_dissolve_fields:
        if field in dissolved.columns:
            dissolved[field] = dissolved[field].astype(str)
    
    # Add Global ID
    dissolved['GlobalID'] = [str(uuid.uuid4()) for _ in range(len(dissolved))]
        
    logger.info("   Processing complete")
    return dissolved


def transform_treatments(enriched_polygons, enriched_lines, enriched_points):

    logger = logging.getLogger('process.tran_treatment')
    
    logger.info("Transform Treatments ...")
    
    # All possible dissolve fields
    all_dissolve_fields = [
        "TRMTID_USER", "PROJECTID_USER", "PROJECTID", "PROJECTNAME_",
        "ORG_ADMIN_t", "PRIMARY_OWNERSHIP_GROUP", "PRIMARY_OBJECTIVE",
        "SECONDARY_OBJECTIVE", "TERTIARY_OBJECTIVE", "TREATMENT_STATUS",
        "COUNTY", "IN_WUI", "REGION", "TREATMENT_START", "TREATMENT_END",
        "RETREATMENT_DATE_EST", "TREATMENT_NAME", "BatchID", "Val_Status_t",
        "Val_Msg_t", "Val_RunDate_t", "Review_Status_t", "Review_Msg_t",
        "Review_RunDate_t", "Dataload_Status_t", "Dataload_Msg_t"
    ]

    def process_geodataframe(
        gdf: gpd.GeoDataFrame,
        geometry_type: str
    ) -> gpd.GeoDataFrame:
        """Helper function to process each geodataframe"""
        
        logger.info(f"      Processing {geometry_type} features")
        
        # Get only the dissolve fields that exist in the dataframe
        available_dissolve_fields = [field for field in all_dissolve_fields if field in gdf.columns]
        
        if not available_dissolve_fields:
            logger.warning(f"No dissolve fields found in {geometry_type} data. Using all columns for dissolve.")
            available_dissolve_fields = list(gdf.columns)
            # Remove geometry column if it exists in the list
            if 'geometry' in available_dissolve_fields:
                available_dissolve_fields.remove('geometry')
        
        logger.info(f"      Using dissolve fields: {available_dissolve_fields}")
        
        try:
            # Create temporary UID for dissolving
            gdf["TEMP_UID"] = gdf.set_index(available_dissolve_fields).index.factorize()[0]
            
            # Dissolve features
            logger.info(f"      Dissolving {geometry_type} features by UID")
            dissolved = gdf.dissolve(by="TEMP_UID").reset_index(drop=True)
            
            # Convert geometries to Multi-type
            if geometry_type == "polygon":
                dissolved["geometry"] = [
                    MultiPolygon([feature]) if isinstance(feature, Polygon)
                    else feature for feature in dissolved["geometry"]
                ]
            elif geometry_type == "point":
                dissolved["geometry"] = [
                    MultiPoint([feature]) if isinstance(feature, Point)
                    else feature for feature in dissolved["geometry"]
                ]
            else:  # line
                dissolved["geometry"] = [
                    MultiLineString([feature]) if isinstance(feature, LineString)
                    else feature for feature in dissolved["geometry"]
                ]
            
            # Cast existing fields to string to handle NaN values
            for field in available_dissolve_fields:
                if field in dissolved.columns:
                    dissolved[field] = dissolved[field].astype(str)
            
            # Add TREATMENT_AREA field
            dissolved["TREATMENT_AREA"] = 0.0  # Initialize field
            
            # Calculate area for polygons (in acres)
            if geometry_type == "polygon":
                # Convert area from square meters to acres (1 sq meter = 0.000247105 acres)
                dissolved["TREATMENT_AREA"] = dissolved.geometry.area * 0.000247105
                
            # Add Global ID
            dissolved["GLOBALID"] = [str(uuid.uuid4()) for _ in range(len(dissolved))]
            
            return dissolved
            
        except Exception as e:
            logger.error(f"Error processing {geometry_type} features: {str(e)}")
            raise
    
    # Process each geometry type
    logger.info("   Starting polygon processing")
    processed_polygons = process_geodataframe(enriched_polygons, "polygon")
    
    logger.info("   Starting point processing")
    processed_points = process_geodataframe(enriched_points, "point")
    
    logger.info("   Starting line processing")
    processed_lines = process_geodataframe(enriched_lines, "line")
    
    logger.info("   Processing complete")
    
    return processed_polygons, processed_points, processed_lines


def transform_activities(enriched_polygons, enriched_lines, enriched_points):

    logger = logging.getLogger('process.tran_activity')
    
    logger.info("Transform Activities ...")

    # Fields to drop
    fields_to_drop = [
        "GlobalID", "ACTIVID_USER", "TRMTID_USER", "TREATMENTID_", "ORG_ADMIN_a",
        "ACTIVITY_DESCRIPTION", "ACTIVITY_CAT",  "BROAD_VEGETATION_TYPE",
        "BVT_USERD", "ACTIVITY_STATUS", "ACTIVITY_QUANTITY", "ACTIVITY_UOM",
        "ACTIVITY_START", "ACTIVITY_END", "ADMIN_ORG_NAME", "IMPLEM_ORG_NAME",
        "PRIMARY_FUND_SRC_NAME", "PRIMARY_FUND_ORG_NAME", "SECONDARY_FUND_SRC_NAME",
        "SECONDARY_FUND_ORG_NAME", "TERTIARY_FUND_SRC_NAME", "TERTIARY_FUND_ORG_NAME",
        "ACTIVITY_PRCT", "RESIDUE_FATE", "RESIDUE_FATE_QUANTITY", "RESIDUE_FATE_UNITS",
        "ACTIVITY_NAME", "VAL_STATUS_a", "VAL_MSG_a", "VAL_RUNDATE_a", "REVIEW_STATUS_a",
        "REVIEW_MSG_a", "REVIEW_RUNDATE_a", "DATALOAD_STATUS_a", "DATALOAD_MSG_a",
        "TRMT_GEOM", "COUNTS_TO_MAS"
    ]

    try:
        # Convert GeoDataFrames to regular DataFrames if needed
        logger.info("   Converting point features to table")
        points_df = enriched_points.drop(columns='geometry') if isinstance(enriched_points, gpd.GeoDataFrame) else enriched_points.copy()
        
        logger.info("   Converting line features to table")
        lines_df = enriched_lines.drop(columns='geometry') if isinstance(enriched_lines, gpd.GeoDataFrame) else enriched_lines.copy()
        
        logger.info("   Converting polygon features to table")
        poly_df = enriched_polygons.drop(columns='geometry') if isinstance(enriched_polygons, gpd.GeoDataFrame) else enriched_polygons.copy()
        
        # Combine points and lines first
        logger.info("   Combining point and line tables")
        combined_df = pd.concat([points_df, lines_df], ignore_index=True)
        
        # Append to polygons
        logger.info("   Appending combined table to polygon table")
        final_df = pd.concat([poly_df, combined_df], ignore_index=True)
        
        # Add Global IDs
        logger.info("   Adding Global IDs")
        final_df['GLOBALID'] = [str(uuid.uuid4()) for _ in range(len(final_df))]
        
        # Drop specified fields if they exist
        existing_fields_to_drop = [field for field in fields_to_drop if field in final_df.columns]
        if existing_fields_to_drop:
            logger.info(f"   Dropping {len(existing_fields_to_drop)} fields")
            final_df = final_df.drop(columns=existing_fields_to_drop)
        else:
            logger.info("   No fields to drop found in the table")
        
        logger.info("   Table processing complete")
        return final_df
    except Exception as e:
        logger.error(f"   Error processing tables: {str(e)}")
        raise
