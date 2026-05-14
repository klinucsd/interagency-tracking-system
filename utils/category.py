
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
            'MECH_HFR', 'PRESCRB_FIRE', 'GRAZING', 'LAND_PROTEC',  
            'TIMB_HARV', 'TREE_PLNTING'
        }
        if Act in direct_activities:
            return Act
            
        # Mechanical Hazardous Fuel Reduction activities
        mech_hfr_activities = {
            "WATSHD_IMPRV", "BIOMASS_REMOVAL", "CHIPPING", "CHAIN_CRUSH", "DISCING", "DOZER_LINE", "HANDLINE", 
            "LANDING_TRT", "LOP_AND_SCAT", "MASTICATION", "MOWING", "PILING", "PRUNING", 'ROAD_CLEAR',  
            "SLASH_DISPOSAL", "THIN_MAN", "THIN_MECH", "TREE_RELEASE_WEED", "TREE_FELL", "UTIL_RIGHTOFWAY_CLR", 
            "YARDING", "PEST_CNTRL"
        }
        if Act in mech_hfr_activities:
            return "MECH_HFR"
            
        # Timber harvest activities
        timber_harvest_activities = {
            "CLEARCUT", "COMM_THIN", "CONVERSION", "GRP_SELECTION_HARVEST", 
            "REHAB_UNDRSTK_AREA", "SEED_TREE_PREP_STEP", "SEED_TREE_REM_STEP", "SEED_TREE_SEED_STEP", 
            "SHELTERWD_PREP_STEP", "SHELTERWD_REM_STEP", "SHELTERWD_SEED_STEP", "SINGLE_TREE_SELECTION", 
            "SP_PRODUCTS", "TRANSITION_HARVEST", "VARIABLE_RETEN_HARVEST"
        }
        if Act in timber_harvest_activities:
            return "TIMB_HARV"
        
        # Pest control logic
        if Act in {"SALVG_HARVEST", 'SANI_HARVEST'}:
            return "SANI_SALVG"
            
        # Watershed improvement activities
        if Act in {"INV_PLANT_REMOVAL", "ECO_HAB_RESTORATION"}:
            return "MECH_HFR"
            
        # Herbicide application logic
        if not pd.isna(Act) and Act == "HERBICIDE_APP":
                
            tree_planting_objectives = {
                "FOREST_PEST_CNTRL", "FOREST_STEWARDSHIP",
                "OTHER_FOREST_MGMT", "REFORESTATION", "SITE_PREP"
            }
            if Obj in tree_planting_objectives:
                return "TREE_PLNTING"
                

            return "MECH_HFR"
                
        # Tree planting activities
        if Act in {"SITE_PREP", "TREE_PLNTING", "TREE_SEEDING"}:
            return "TREE_PLNTING"
            
        # Beneficial fire activities
        if Act in {"PILE_BURN", "BROADCAST_BURN", "PL_TREAT_BURNED", "WM_RESRC_BENEFIT", "BENEFICIAL_FIRE"}:
            return "PRESCRB_FIRE"
            
        # Grazing activities
        if not pd.isna(Act) and Act == "PRESCRB_HERBIVORY":
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
            return "MECH_HFR"
            
        # Default case
        print(Act)
        print(Obj)
        return "NOT_DEFINED"
    
    # Create a copy of the input GeoDataFrame
    result_gdf = gdf.copy()
    
    # Apply the classification function
    result_gdf['ACTIVITY_CAT'] = result_gdf.apply(classify_activity, axis=1)
    
    return result_gdf

