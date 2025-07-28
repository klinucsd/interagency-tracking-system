import pandas as pd
import geopandas as gpd
import numpy as np
from shapely.geometry import Polygon
import matplotlib.pyplot as plt
import timeit
import time

# Control flow parameters
flag_run_main = False
flag_piecemeal_plot = True

# Initialize time tracking
te = [timeit.default_timer()]
i_t = 0

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# 1) CREATE DEMONSTRATION GEOMETRIES

# Define essential funcitons
def square_from_center(cx, cy, L):
    half = L / 2
    corners = [
        (cx - half, cy - half),
        (cx + half, cy - half),
        (cx + half, cy + half),
        (cx - half, cy + half)
    ]
    return Polygon(corners)

def calc_likelihood(p_vec):
    return 1 - np.prod(1 - p_vec)

# Deifne GeoDataFrame parameters
L_vec = [50, 40, 30, 20, 10, 20, 30, 10]
X_vec = [30, 60, 35, 80, 20, 20, 80, 65]
Y_vec = [30, 60, 60, 20, 20, 90, 85, 90]
p_vec = [0.5, 0.6, 0.7, 0.8, 0.9, 0.5, 0.6, 0.7]

# Create a list of squares
squares = [square_from_center(X_vec[i], Y_vec[i], L_vec[i]) for i in range(len(L_vec))]

# Create a GeoDataFrame
squares_gdf = gpd.GeoDataFrame({
    'p_i': p_vec,
    'geometry': squares})

squares_gdf['FID'] = "S" + squares_gdf.index.astype(str)

# CONCEPTUAL WORKFLOW
# 1) Find and exclude any non-intersecting features and set aside [= non_inter_gdf]
#   --> If set of intersecting features is empty, return non_inter_gdf
# 2) Loop through each potentially intersecting feature to find non-overlapping polygons
# 3) Find potential neighbors based on intersecting bounding boxes
# 4) Find actual set of intersecting neighbords from the set of potential neighbors
# 5) Find singular 'difference' NOP of independent of intersecting neighbors and set aside [= diff_nop_gdf]
# 6) Find one to many intersecting NOP with neighbors [= inter_nop_gdf]
# 7) Recursively repeat steps #1->#6 on just the intersecting features until no more non-overlapping 
#   polygons are found [**See Step #1 on how to break the recursion**]
# 8) Concatenate non_inter_gdf + diff_nop_gdf + recursively found inter_nop_gdf_list to create a 
#   composite GeoDataFrame [nop_gdf]
# 9) Find and remove duplicates created by bidirectional pairwise intersections
# 10) Return the final GeoDataFrame

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# 2) CREATE CORE FUNCTIONS 

# Function to find local intersections between a target feature and a set of features
def get_interaction_list(gdf):
    # Build spatial index for efficient bounding box queries
    sindex = gdf.sindex

    # Store results in a pre-formatted list
    interaction_gdf_list = [None]*len(gdf)

    for idx, row in gdf.iterrows():
        # Get the bounding box of the current feature
        bounds = row.geometry.bounds

        # Find possible neighbors using bounding box intersection
        possible_neighbors = list(sindex.intersection(bounds))

        # Remove self from possible neighbors
        possible_neighbors = [i for i in possible_neighbors if i != idx]

        # Check for actual intersection with each possible neighbor
        intersecting_neighbors = [
            i for i in possible_neighbors
            if row.geometry.intersects(gdf.loc[i].geometry)
        ]

        # Assign results if not empty
        if len(intersecting_neighbors) > 0:
            interaction_gdf_list[idx] = gdf.iloc[intersecting_neighbors]
        else:
            interaction_gdf_list[idx] = []

    return interaction_gdf_list

# Function to remove duplicate geometries with different coordinate vectors
def remove_duplicates(gdf):
    normalized_wkb = gdf.geometry.apply(lambda geom: geom.normalize().wkb)
    unique_gdf = gdf.loc[~normalized_wkb.duplicated()].reset_index(drop=True)

    return unique_gdf

# Function to recursively decompose a set of features into non-overlapping polygons (NOPs)
def recursive_nop_decomposition(features_gdf):

    # Loop through all features to find full sets of intersecting and non-intersecting features
    interaction_gdf_list = get_interaction_list(features_gdf)

    interaction_gdf_rows_list = [len(gdf) for gdf in interaction_gdf_list]
    non_intersecting_indices = [i for i, count in enumerate(interaction_gdf_rows_list) if count == 0]
    intersecting_indices  = [i for i, count in enumerate(interaction_gdf_rows_list) if count > 0]

    if len(non_intersecting_indices) > 0:
        non_intersecting_gdf = features_gdf.iloc[non_intersecting_indices].copy()

    # Test to see if any intersecting features were found
    if len(intersecting_indices) == 0:
        return features_gdf
    
    else:
        # Define intersecting features of interest and their set of intersecting neighbors
        intersecting_features_gdf = features_gdf.iloc[intersecting_indices].copy().reset_index(drop=True)
        intersecting_gdf_list = [interaction_gdf_list[i] for i in intersecting_indices]

        nop_gdf_list = []
        
        #Loop through the remaining sub-set of intersecting features
        for idx, row in intersecting_features_gdf.iterrows():
            lvl2_target_gdf = intersecting_features_gdf.loc[[idx]]
            lvl2_neighbors_gdf = intersecting_gdf_list[idx]
            
            diff_nop_gdf = lvl2_target_gdf.overlay(
                lvl2_neighbors_gdf, 
                how='difference', 
                keep_geom_type=False)
            
            inter_nop_gdf = lvl2_target_gdf.overlay(
                lvl2_neighbors_gdf, 
                how='intersection', 
                keep_geom_type=False)

            # Recursively decompose any intersecting intersections
            lvl2_inter_nop_gdf = recursive_nop_decomposition(inter_nop_gdf)

            # Compile all NOPs for a given target feature in the NOP list
            nop_gdf_list.append(pd.concat([diff_nop_gdf, lvl2_inter_nop_gdf], ignore_index=True))

        # Concatenate all independent NOPs found plus set of non-intersecting features
        nop_gdf = pd.concat(nop_gdf_list, ignore_index=True)

        if len(non_intersecting_indices) > 0:
            nop2_gdf = pd.concat([non_intersecting_gdf, nop_gdf], ignore_index=True)
        else:
            nop2_gdf = nop_gdf

        # Get rid of duplicate geometries created by bidirectional pairwise intersections
        unique_nop_gdf = remove_duplicates(nop2_gdf)

        return unique_nop_gdf


#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# 3) MAIN PROGRAM

if flag_run_main:
    squares_red_gdf = squares_gdf[['geometry']].copy()

    all_nop_gdf = recursive_nop_decomposition(squares_red_gdf)
    all_nop_gdf = remove_duplicates(all_nop_gdf)

    # Track time of calculation
    te.append(timeit.default_timer())
    i_t += 1
    print(f">> Finished calculation in: {round(te[i_t]-te[0], 2)} seconds")

    # PLOT RESULTS
    all_nop_centroids = all_nop_gdf.geometry.representative_point()

    # Setup plot axes
    my_fig, my_ax = plt.subplots(figsize=(8, 8))

    # Plot each NOP one by one
    for i in range(len(all_nop_gdf)):
        all_nop_gdf.iloc[[i]].plot(ax=my_ax, color='purple', alpha=0.5, edgecolor='black')
        my_ax.set_title(f"NOP {i}")
        my_ax.set_xlim([0, 110])
        my_ax.set_ylim([0, 110])
        plt.pause(0.5)

    # Clear the axes for the final plot
    my_ax.cla()
    all_nop_gdf.plot(ax=my_ax, cmap='viridis', alpha=0.5, edgecolor='black')
    all_nop_centroids.plot(ax=my_ax, color='red', markersize=3, label='Centroids')

print(">> **Finished Script**")
