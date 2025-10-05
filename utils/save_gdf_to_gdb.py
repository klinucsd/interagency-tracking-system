
import os
import subprocess
import traceback
import logging


logger = logging.getLogger(__name__)

# Function to save a GeoDataFrame to a File Geodatabase
def save_gdf_to_gdb(gdf, output_gdb, layer_name, group_name=None):
    temp_geojson = "/tmp/temp_output.geojson"
    gdf.to_file(temp_geojson, driver="GeoJSON")
    
    # Determine geometry type
    geom_type = gdf.geometry.geom_type.unique()[0].upper()
    geom_map = {
        'POINT': 'POINT',
        'MULTIPOINT': 'MULTIPOINT',
        'LINESTRING': 'LINESTRING',
        'MULTILINESTRING': 'MULTILINESTRING',
        'POLYGON': 'POLYGON',
        'MULTIPOLYGON': 'MULTIPOLYGON'
    }
    gdal_geom = geom_map.get(geom_type, 'UNKNOWN')
    
    cmd = [
        "ogr2ogr",
        "-f", "FileGDB",
        output_gdb,
        temp_geojson,
        "-nlt", gdal_geom,
        "-nln", layer_name,
        "-overwrite"
    ]
    
    # Use group_name if needed
    # if group_name:
    #     cmd.extend(["-lco", f"FEATURE_DATASET={group_name}"])
    
    subprocess.run(cmd, check=True)
    os.remove(temp_geojson)

# Function to save a GeoDataFrame to a File Geodatabase
def save_gdf_to_gdb_2(gdf, output_gdb, layer_name, group_name=None):
    # Get the current user and group IDs
    user_id = os.getuid()
    group_id = os.getgid()

    # Construct the full paths
    data_dir = os.path.dirname(output_gdb)
    output_gdb_name = os.path.basename(output_gdb)

    # Save GeoDataFrame to a temporary GeoJSON file
    temp_geojson = f"{data_dir}/temp_output.geojson"
    logger.info(f"      Data dimension: {gdf.shape}")
    logger.info(f"      Dumping data as temporary Geojson: {temp_geojson}")
    gdf.to_file(temp_geojson, driver="GeoJSON")

    # Get unique geometry types in the GeoDataFrame
    geometry_types = gdf.geometry.geom_type.unique()

    # Convert GeoPandas geometry type to GDAL geometry type
    gdal_geometry_mapping = {
        'Point': 'POINT',
        'MultiPoint': 'MULTIPOINT',
        'LineString': 'LINESTRING',
        'MultiLineString': 'MULTILINESTRING',
        'Polygon': 'POLYGON',
        'MultiPolygon': 'MULTIPOLYGON'
    }

    # Get the GDAL geometry type
    gdal_geometry_type = gdal_geometry_mapping[geometry_types[0]]
    
    del gdf

    # Command to drop the existing layer
    drop_cmd = [
        "docker", "run", "--rm",
        f"--user={user_id}:{group_id}",
        "-v", f"{data_dir}:/data",
        "dbcawa/gdal-image:latest",
        "ogrinfo",
        f"/data/{output_gdb_name}",
        "-sql", f'DROP TABLE "{layer_name}"'
    ]
    
    # Construct the Docker command
    add_cmd = [
        "docker", "run", "--rm",
        f"--user={user_id}:{group_id}",
        "-v", f"{data_dir}:/data",
        "dbcawa/gdal-image:latest",
        "ogr2ogr",
        "-overwrite",
        "-update",
        # "-append",
        "-f", "FileGDB",
        f"/data/{output_gdb_name}",
        f"/data/temp_output.geojson",
        "-nlt", gdal_geometry_type,
        "-nln", layer_name
    ]

    # Add feature dataset option if group_name is provided
    if group_name:
        add_cmd.extend(["-lco", f"FEATURE_DATASET={group_name}"])

    # Run the command
    try:
        logger.info(f"      Running drop command: {' '.join(drop_cmd)}")
        subprocess.run(drop_cmd, check=False, capture_output=True, text=True)

        logger.info(f"      Running add command: {' '.join(add_cmd)}")
        result = subprocess.run(add_cmd, check=True, capture_output=True, text=True)
             
        logger.info(f"      GeoDataFrame successfully saved to File Geodatabase.")
        logger.info(f"      Command output: {result.stdout}")
    except subprocess.CalledProcessError as e:
        logger.error(f"      An error occurred while running the Docker command.")
        logger.error(f"      Command output: {e.stdout}")
        logger.error(f"      Error output: {e.stderr}")
        traceback.print_exc()
    finally:
        # Clean up the temporary GeoJSON file
        if os.path.exists(temp_geojson):
            os.remove(temp_geojson)
