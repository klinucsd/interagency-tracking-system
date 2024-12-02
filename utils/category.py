
import geopandas as gpd
import pandas as pd


def categorize_activity(gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    """
    Categorize activities based on ACTIVITY_DESCRIPTION, BROAD_VEGETATION_TYPE, and PRIMARY_OBJECTIVE fields.
    
    Parameters:
    -----------
    gdf : geopandas.GeoDataFrame
        Input GeoDataFrame containing the required fields
        
    Returns:
    --------
    geopandas.GeoDataFrame
        GeoDataFrame with new ACTIVITY_CAT field
    """
    def classify_activity(row):
        Act = row['ACTIVITY_DESCRIPTION']
        Veg = row['BROAD_VEGETATION_TYPE']
        Obj = row['PRIMARY_OBJECTIVE']
        
        # Direct classifications
        direct_activities = {
            'MECH_HFR', 'BENEFICIAL_FIRE', 'GRAZING', 'LAND_PROTEC',
            'TIMB_HARV', 'TREE_PLNTING', 'WATSHD_IMPRV'
        }
        if Act in direct_activities:
            return Act
            
        # Mechanical Hazardous Fuel Reduction activities
        mech_hfr_activities = {
            "BIOMASS_REMOVAL", "CHIPPING", "CHAIN_CRUSH", "DISCING",
            "DOZER_LINE", "HANDLINE", "LANDING_TRT", "LOP_AND_SCAT",
            "MASTICATION", "MOWING", "PILE_BURN", "PILING", "PRUNING",
            'ROAD_CLEAR', "SLASH_DISPOSAL", "THIN_MAN", "THIN_MECH",
            "TREE_RELEASE_WEED", "TREE_FELL", "UTIL_RIGHTOFWAY_CLR", "YARDING"
        }
        if Act in mech_hfr_activities:
            return "MECH_HFR"
            
        # Timber harvest activities
        timber_harvest_activities = {
            "CLEARCUT", "COMM_THIN", "CONVERSION", "GRP_SELECTION_HARVEST",
            "REHAB_UNDRSTK_AREA", "SEED_TREE_PREP_STEP", "SEED_TREE_REM_STEP",
            "SEED_TREE_SEED_STEP", "SHELTERWD_PREP_STEP", "SHELTERWD_REM_STEP",
            "SHELTERWD_SEED_STEP", "SINGLE_TREE_SELECTION", "SP_PRODUCTS",
            "TRANSITION_HARVEST", "VARIABLE_RETEN_HARVEST", "SALVG_HARVEST",
            "SANI_HARVEST"
        }
        if Act in timber_harvest_activities:
            return "TIMB_HARV"
            
        # Pest control logic
        if Act == "PEST_CNTRL":
            return "SANI_SALVG" if Veg == "FOREST" else "WATSHD_IMPRV"
            
        # Watershed improvement activities
        if Act in {"INV_PLANT_REMOVAL", "ECO_HAB_RESTORATION"}:
            return "WATSHD_IMPRV"
            
        # Herbicide application logic
        if Act == "HERBICIDE_APP":
            watershed_objectives = {
                "BURNED_AREA_RESTOR", "CARBON_STORAGE", "ECO_RESTOR",
                "HABITAT_RESTOR", "INV_SPECIES_CNTRL", "LAND_PROTECTION",
                "MTN_MEADOW_RESTOR", "RIPARIAN_RESTOR", "WATSHD_RESTOR",
                "WETLAND_RESTOR"
            }
            if Obj in watershed_objectives:
                return "WATSHD_IMPRV"
                
            tree_planting_objectives = {
                "FOREST_PEST_CNTRL", "FOREST_STEWARDSHIP",
                "OTHER_FOREST_MGMT", "REFORESTATION", "SITE_PREP"
            }
            if Obj in tree_planting_objectives:
                return "TREE_PLNTING"
                
            mech_hfr_objectives = {
                "BIOMASS_UTIL", "CULTURAL_BURN", "FIRE_PREVENTION",
                "FUEL_BREAK", "NON-TIMB_PRODUCTS", "OTHER_FUELS_REDUCTION",
                "PRESCRB_FIRE", "RECREATION", "ROADWAY_CLEARANCE",
                "TIMBER_HARVEST", "UTIL_RIGHT_OF_WAY"
            }
            if Obj in mech_hfr_objectives:
                return "MECH_HFR"
                
        # Tree planting activities
        if Act in {"SITE_PREP", "TREE_PLNTING", "TREE_SEEDING"}:
            return "TREE_PLNTING"
            
        # Beneficial fire activities
        if Act in {"BROADCAST_BURN", "PL_TREAT_BURNED", "WM_RESRC_BENEFIT"}:
            return "BENEFICIAL_FIRE"
            
        # Grazing activities
        if Act == "PRESCRB_HERBIVORY":
            return "GRAZING"
            
        # Land protection activities
        if Act in {"EASEMENT", "FEE_TITLE", "LAND_ACQ"}:
            return "LAND_PROTEC"
            
        # Watershed improvement activities
        if Act in {
            "AMW_AREA_RESTOR", "EROSION_CONTROL", "HABITAT_REVEG",
            "OAK_WDLND_MGMT", "ROAD_OBLITERATION", "SEEDBED_PREP",
            "STREAM_CHNL_IMPRV", "WETLAND_RESTOR"
        }:
            return "WATSHD_IMPRV"
            
        # Default case
        return "NOT_DEFINED"
    
    # Create a copy of the input GeoDataFrame
    result_gdf = gdf.copy()
    
    # Apply the classification function
    result_gdf['ACTIVITY_CAT'] = result_gdf.apply(classify_activity, axis=1)
    
    return result_gdf

