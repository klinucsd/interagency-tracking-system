import pandas as pd
import geopandas as gpd

def process_chunk(chunk, gdf):
    return gpd.sjoin_nearest(chunk, gdf, how='left')

