
import pandas as pd
import geopandas as gpd
import numpy as np


def update_agency_field(agency):
    if agency == 'CA Environmental Protection Agency':
        return 'CALEPA'
    elif agency == 'CA State Transportation Agency':
        return 'CALSTA'
    elif agency == 'CA Natural Resources Agency':
        return 'CNRA'
    elif agency in ['U.S. Department of Defense', 'DoD']:
        return 'DOD'
    elif agency == 'Department of the Interior':
        return 'DOI'
    elif agency == 'Department of Agriculture':
        return 'USDA'
    elif agency == 'Other':
        return 'OTHER'
    elif agency == 'Industrial Timber':
        return 'TIMBER'
    elif agency in ['', ' ']:
        return None
    else:
        return agency

    
def update_org_field(org):
    org_mapping = {
        'Baldwin Hills Conservancy': 'BHC',
        'Bureau of Indian Affairs': 'BIA',
        'Bureau of Land Management': 'BLM',
        'CA Board of Forestry and Fire Protection': 'BOF',
        'CAL FIRE': 'CALFIRE',
        'CA Department of Transportation': 'CALTRANS',
        'CA Air Resources Board': 'CARB',
        'CA Conservation Corps': 'CCC',
        'CA Department of Fish and Wildlife': 'CDFW',
        'Coachella Valley Mountains Conservancy': 'CVMC',
        'CA Department of Conservation': 'DOC',
        'DoD': 'DOD',
        'U.S. Department of Defense': 'DOD',
        'CA Department of Water Resources': 'DWR',
        'US Fish and Wildlife Service': 'FWS',
        'Mountains Recreation and Conservation Authority': 'MRCA',
        'National Park Service': 'NPS',
        'Natural Resources Conservation Service': 'NRCS',
        'Office of Energy Infrastructure Safety': 'OEIS',
        'CA State Parks': 'PARKS',
        'San Gabriel and Lower Los Angeles Rivers and Mountains Conservancy': 'RMC',
        'State Coastal Conservancy': 'SCC',
        'San Diego River Conservancy': 'SDRC',
        'State Lands Commission': 'SLC',
        'Santa Monica Mountains Conservancy': 'SMMC',
        'Sierra Nevada Conservancy': 'SNC',
        'Tahoe Conservancy': 'TAHOE',
        'U.S. Forest Service': 'USFS',
        'CA Wildlife Conservation Board': 'WCB',
        'State Water Resources Control Board': 'WRCB',
        'Other': 'OTHER',
        'Timber Companies': 'TIMBER',
    }

    if pd.isna(org):  # Check if the value is missing (pd.NA, np.nan, or None)
        return None
    return org_mapping.get(org, None if org in ['', ' '] else org)


def update_project_status_field(status):

    if pd.isna(status):  # Handles pd.NA, np.nan, or None
        return None  
    
    if status in ['Active', 'Active*']:
        return 'ACTIVE'
    elif status == 'Complete':
        return 'COMPLETE'
    elif status == 'Cancelled':
        return 'CANCELLED'
    elif status == 'Outyear':
        return 'OUTYEAR'
    elif status == 'Planned':
        return 'PLANNED'
    elif status == 'Proposed':
        return 'PROPOSED'
    elif status in ['', ' ']:
        return None
    else:
        return status


def update_primary_funding_source(org):

    if pd.isna(org):  # Handles pd.NA, np.nan, or None
        return None  
    
    if org == 'Greenhouse Gas Reduction Fund':
        return 'GHG_REDUC_FUND_GGRF'
    elif org == 'Proposition 68 Bond Funds':
        return 'PROP_68_BOND_FUNDS'
    elif org == 'SB 170 (2021) Wildfire Resilience Fund: General Fund':
        return 'GENERAL_FUND_SB170_2021'
    elif org == 'SB 170 (2021) Wildfire Resilience Fund: GGRF':
        return 'GGRF_SB170_2021'
    elif org == 'SB 85 (2021) Wildfire Resilience Early Action: General Fund':
        return 'GENERAL_FUND_SB85_2021'
    elif org == 'SB 85 (2021) Wildfire Resilience Early Action: GGRF':
        return 'GGRF_SB85_2021'
    elif org == 'State General Fund':
        return 'GENERAL_FUND'
    elif org == 'Other State Funds':
        return 'OTHER_STATE_FUNDS'
    elif org == 'Federal':
        return 'FEDERAL'
    elif org == 'Local':
        return 'LOCAL'
    elif org == 'Private':
        return 'PRIVATE'
    elif org == '' or org == ' ':
        return None
    else:
        return org
    

def update_primary_ownership_group(ownership):
    if ownership == 'Federal':
        return 'FEDERAL'
    elif ownership == 'Local':
        return 'LOCAL'
    elif ownership == 'NGO':
        return 'NGO'
    elif ownership == 'Private - Industrial':
        return 'PRIVATE_INDUSTRY'
    elif ownership == 'Private - Non-Industrial':
        return 'PRIVATE_NON-INDUSTRY'
    elif ownership == 'State':
        return 'STATE'
    elif ownership == 'Tribal':
        return 'TRIBAL'
    elif ownership in ['', ' ']:
        return None
    else:
        return ownership


def update_objective(obj):
    # List of valid objectives
    valid_objectives = [
        'BIOMASS_UTIL', 'BURNED_AREA_RESTOR', 'CARBON_STORAGE', 'CULTURAL_BURN', 
        'ECO_RESTOR', 'FIRE_PREVENTION', 'FOREST_PEST_CNTRL', 'FOREST_STEWARDSHIP',
        'FUEL_BREAK', 'HABITAT_RESTOR', 'INV_SPECIES_CNTRL', 'LAND_PROTECTION',
        'MTN_MEADOW_RESTOR', 'NON-TIMB_PRODUCTS', 'OTHER_FOREST_MGMT',
        'OTHER_FUELS_REDUCTION', 'PRESCRB_FIRE', 'RECREATION', 'REFORESTATION',
        'RIPARIAN_RESTOR', 'ROADWAY_CLEARANCE', 'SITE_PREP', 'TIMBER_HARVEST',
        'UTIL_RIGHT_OF_WAY', 'WATSHD_RESTOR', 'WETLAND_RESTOR', 'NOT_DEFINED'
    ]
    
    # Mapping for common replacements
    replacement_map = {
        'Biomass Utilization': 'BIOMASS_UTIL',
        'Burned Area Restoration': 'BURNED_AREA_RESTOR',
        'Carbon Storage': 'CARBON_STORAGE',
        'Cultural Burn': 'CULTURAL_BURN',
        'Ecological Restoration': 'ECO_RESTOR',
        'Fire Prevention': 'FIRE_PREVENTION',
        'Forest Pest Control': 'FOREST_PEST_CNTRL',
        'Forestland Stewardship': 'FOREST_STEWARDSHIP',
        'Fuel Break': 'FUEL_BREAK',
        'Habitat Restoration': 'HABITAT_RESTOR',
        'Invasive Species Control': 'INV_SPECIES_CNTRL',
        'Land Protection': 'LAND_PROTECTION',
        'Mountain Meadow Restoration': 'MTN_MEADOW_RESTOR',
        'Non-Timber Products': 'NON-TIMB_PRODUCTS',
        'Other Forest Management': 'OTHER_FOREST_MGMT',
        'Other Fuels Reduction': 'OTHER_FUELS_REDUCTION',
        'Prescribed Fire': 'PRESCRB_FIRE',
        'Recreation': 'RECREATION',
        'Reforestation': 'REFORESTATION',
        'Riparian Restoration': 'RIPARIAN_RESTOR',
        'Roadway Clearance': 'ROADWAY_CLEARANCE',
        'Site Preparation': 'SITE_PREP',
        'Timber Harvest': 'TIMBER_HARVEST',
        'Utility Right of Way Clearance': 'UTIL_RIGHT_OF_WAY',
        'Watershed Restoration': 'WATSHD_RESTOR',
        'Wetland Restoration': 'WETLAND_RESTOR',
        'Not Defined': 'NOT_DEFINED'
    }
    

    # Handle missing or NA values
    if pd.isna(obj):
        return 'TBD'

    # Apply mapping logic
    if obj in valid_objectives:
        return obj
    elif obj in replacement_map:
        return replacement_map[obj]
    elif obj is None or str(obj).strip() == '':
        return 'TBD'
    else:
        return obj


def update_treatment_status(stat):

    # Handle missing or NA values                                                                                                                                  
    if pd.isna(stat):
        return None

    if stat in ['Active', 'Active*']:
        return 'ACTIVE'
    elif stat == 'Complete':
        return 'COMPLETE'
    elif stat == 'Cancelled':
        return 'CANCELLED'
    elif stat == 'Outyear':
        return 'OUTYEAR'
    elif stat == 'Planned':
        return 'PLANNED'
    elif stat == 'Proposed':
        return 'PROPOSED'
    elif stat in ['', ' ']:
        return None
    else:
        return stat


def map_county_to_code(county_name):
    county_mapping = {
        "ALA": ["Alameda", "Alameda County", "ALAMEDA"],
        "ALP": ["Alpine", "Alpine County", "ALPINE"],
        "AMA": ["Amador", "Amador County", "AMADOR"],
        "BUT": ["Butte", "Butte County", "BUTTE"],
        "CAL": ["Calaveras", "Calaveras County", "CALAVERAS"],
        "COL": ["Colusa", "Colusa County", "COLUSA"],
        "CC": ["Contra Costa", "Contra Costa County", "CONTRA COSTA"],
        "DN": ["Del Norte", "Del Norte County", "DEL NORTE"],
        "ED": ["El Dorado", "El Dorado County", "EL DORADO"],
        "FRE": ["Fresno", "Fresno County", "FRESNO"],
        "GLE": ["Glenn", "Glenn County", "GLENN"],
        "HUM": ["Humboldt", "Humboldt County", "HUMBOLDT"],
        "IMP": ["Imperial", "Imperial County", "IMPERIAL"],
        "INY": ["Inyo", "Inyo County", "INYO"],
        "KER": ["Kern", "Kern County", "KERN"],
        "KIN": ["Kings", "Kings County", "KINGS"],
        "LAK": ["Lake", "Lake County", "LAKE"],
        "LAS": ["Lassen", "Lassen County", "LASSEN"],
        "LA": ["Los Angeles", "Los Angeles County", "LOS ANGELES"],
        "MAD": ["Madera", "Madera County", "MADERA"],
        "MRN": ["Marin", "Marin County", "MARIN"],
        "MPA": ["Mariposa", "Mariposa County", "MARIPOSA"],
        "MEN": ["Mendocino", "Mendocino County", "MENDOCINO"],
        "MER": ["Merced", "Merced County", "MERCED"],
        "MOD": ["Modoc", "Modoc County", "MODOC"],
        "MON": ["Monterey", "Monterey County", "MONTEREY"],
        "MNO": ["Mono", "Mono County", "MONO"],
        "NAP": ["Napa", "Napa County", "NAPA"],
        "NEV": ["Nevada", "Nevada County", "NEVADA"],
        "ORA": ["Orange", "Orange County", "ORANGE"],
        "PLA": ["Placer", "Placer County", "PLACER"],
        "PLU": ["Plumas", "Plumas County", "PLUMAS"],
        "RIV": ["Riverside", "Riverside County", "RIVERSIDE"],
        "SAC": ["Sacramento", "Sacramento County", "SACRAMENTO"],
        "SBT": ["San Benito", "San Benito County", "SAN BENITO"],
        "SBD": ["San Bernardino", "San Bernardino County", "SAN BERNARDINO"],
        "SD": ["San Diego", "San Diego County", "SAN DIEGO"],
        "SF": ["San Francisco", "San Francisco County", "SAN FRANCISCO"],
        "SJ": ["San Joaquin", "San Joaquin County", "SAN JOAQUIN"],
        "SLO": ["San Luis Obispo", "San Luis Obispo County", "SAN LUIS OBISPO"],
        "SM": ["San Mateo", "San Mateo County", "SAN MATEO"],
        "SB": ["Santa Barbara", "Santa Barbara County", "SANTA BARBARA"],
        "SCL": ["Santa Clara", "Santa Clara County", "SANTA CLARA"],
        "SCR": ["Santa Cruz", "Santa Cruz County", "SANTA CRUZ"],
        "SHA": ["Shasta", "Shasta County", "SHASTA"],
        "SIE": ["Sierra", "Sierra County", "SIERRA"],
        "SIS": ["Siskiyou", "Siskiyou County", "SISKIYOU"],
        "SOL": ["Solano", "Solano County", "SOLANO"],
        "SON": ["Sonoma", "Sonoma County", "SONOMA"],
        "STA": ["Stanislaus", "Stanislaus County", "STANISLAUS"],
        "SUT": ["Sutter", "Sutter County", "SUTTER"],
        "TEH": ["Tehama", "Tehama County", "TEHAMA"],
        "TUO": ["Tuolumne", "Tuolumne County", "TUOLUMNE"],
        "TRI": ["Trinity", "Trinity County", "TRINITY"],
        "TUL": ["Tulare", "Tulare County", "TULARE"],
        "VEN": ["Ventura", "Ventura County", "VENTURA"],
        "YOL": ["Yolo", "Yolo County", "YOLO"],
        "YUB": ["Yuba", "Yuba County", "YUBA"],
        "NON_SPATIAL": ["Non-Spatial", "Statewide"],
    }
    for code, names in county_mapping.items():
        if county_name == code or county_name in names:
            return code
    return None


def update_in_wui(WUI):
    if WUI == 'WUI (user defined)':
        return 'WUI_USER_DEFINED'
    elif WUI == 'Yes':
        return 'WUI_USER_DEFINED'
    elif WUI == 'Non-WUI (user defined)':
        return 'NON-WUI_USER_DEFINED'
    elif WUI == 'No':
        return 'NON-WUI_USER_DEFINED'
    elif WUI == 'WUI (auto populated)':
        return 'WUI_AUTO_POP'
    elif WUI == 'Non-WUI (auto populated)':
        return 'NON-WUI_AUTO_POP'
    elif WUI == 'YES':
        return 'WUI_USER_DEFINED'
    elif WUI == 'NO':
        return 'NON-WUI_USER_DEFINED'
    elif WUI == '' or WUI == ' ':
        return None
    else:
        return WUI


def update_region(Region):
    if Region == 'North Coast' or Region == 'Northern California':
        return 'NORTH_COAST'
    elif Region == 'Sierra Nevada':
        return 'SIERRA_NEVADA'
    elif Region == 'Southern California':
        return 'SOUTHERN_CA'
    elif Region in ['Coastal-Inland', 'Central Coast', 'Central California']:
        return 'CENTRAL_COAST'
    elif Region == 'COASTAL_INLAND':
        return 'CENTRAL_COAST'
    elif Region == 'North Coast-Inland':
        return 'NORTH_COAST'
    elif Region == 'NORTH_COAST_INLAND':
        return 'NORTH_COAST'
    elif Region == 'Sierra-Cascade-Inyo':
        return 'SIERRA_NEVADA'
    elif Region == 'SIERRA_CASCADE_INYO':
        return 'SIERRA_NEVADA'
    elif Region == 'Non-Spatial' or Region == 'Statewide':
        return 'NON_SPATIAL'
    elif Region == '' or Region == ' ':
        return None
    else:
        return Region


def update_activity_description(act):

    if pd.isna(act):
        return "TBD"
    
    if act in [
        'AMW_AREA_RESTOR', 'BIOMASS_REMOVAL', 'BROADCAST_BURN', 'CHAIN_CRUSH', 
        'CHIPPING', 'CLEARCUT', 'COMM_THIN', 'CONVERSION', 'DISCING', 'DOZER_LINE', 
        'EASEMENT', 'ECO_HAB_RESTORATION', 'EROSION_CONTROL', 'FEE_TITLE', 
        'GRP_SELECTION_HARVEST', 'HABITAT_REVEG', 'HANDLINE', 'HERBICIDE_APP', 
        'INV_PLANT_REMOVAL', 'LAND_ACQ', 'LANDING_TRT', 'LOP_AND_SCAT', 
        'MASTICATION', 'MOWING', 'NONCOM_THIN_MAN', 'NONCOM_THIN_MECH', 
        'NOT_DEFINED', 'OAK_WDLND_MGMT', 'PEST_CNTRL', 'PILE_BURN', 'PILING', 
        'PL_TREAT_BURNED', 'PRESCRB_HERBIVORY', 'PRUNING', 'REHAB_UNDRSTK_AREA', 
        'ROAD_CLEAR', 'ROAD_OBLITERATION', 'SALVG_HARVEST', 'SANI_HARVEST', 
        'SEED_TREE_PREP_STEP', 'SEED_TREE_REM_STEP', 'SEED_TREE_SEED_STEP', 
        'SEEDBED_PREP', 'SHELTERWD_PREP_STEP', 'SHELTERWD_REM_STEP', 
        'SHELTERWD_SEED_STEP', 'SINGLE_TREE_SELECTION', 'SITE_PREP', 
        'SLASH_DISPOSAL', 'SP_PRODUCTS', 'STREAM_CHNL_IMPRV', 'THIN_MAN', 
        'THIN_MECH', 'TRANSITION_HARVEST', 'TREE_FELL', 'TREE_PLNTING', 
        'TREE_RELEASE_WEED', 'TREE_SEEDING', 'UTIL_RIGHTOFWAY_CLR', 
        'VARIABLE_RETEN_HARVEST', 'WETLAND_RESTOR', 'WM_RESRC_BENEFIT', 'YARDING'
    ]:
        return act
    elif act == 'Aspen/Meadow/Wet Area Restoration':
        return 'AMW_AREA_RESTOR'
    elif act == 'Biomass Removal':
        return 'BIOMASS_REMOVAL'
    elif act == 'Broadcast Burn':
        return 'BROADCAST_BURN'
    elif act == 'Chaining/Crushing':
        return 'CHAIN_CRUSH'
    elif act == 'Chipping':
        return 'CHIPPING'
    elif act == 'Clearcut':
        return 'CLEARCUT'
    elif act == 'Commercial Thin':
        return 'COMM_THIN'
    elif act == 'Conversion':
        return 'CONVERSION'
    elif act == 'Discing':
        return 'DISCING'
    elif act == 'Dozer':
        return 'DOZER_LINE'
    elif act == 'Easement':
        return 'EASEMENT'
    elif act == 'Erosion Control':
        return 'EROSION_CONTROL'
    elif act == 'Group Selection Harvest':
        return 'GRP_SELECTION_HARVEST'
    elif act == 'Habitat Revegetation':
        return 'HABITAT_REVEG'
    elif act == 'Handline':
        return 'HANDLINE'
    elif act == 'Herbicide Application':
        return 'HERBICIDE_APP'
    elif act == 'Invasive Plant Removal':
        return 'INV_PLANT_REMOVAL'
    elif act == 'Land Acquisitions':
        return 'LAND_ACQ'
    elif act == 'Landing Treated - Area Mitigated':
        return 'LANDING_TRT'
    elif act == 'Lop and Scatter':
        return 'LOP_AND_SCAT'
    elif act == 'Mastication':
        return 'MASTICATION'
    elif act == 'Mowing':
        return 'MOWING'
    elif act == 'Noncommercial Thinning (Mechanical)':
        return 'NONCOM_THIN_MECH'
    elif act == 'Noncommercial Thinning (Manual)':
        return 'NONCOM_THIN_MAN'
    elif act == 'Oak Woodland Management':
        return 'OAK_WDLND_MGMT'
    elif act == 'Pest Control':
        return 'PEST_CNTRL'
    elif act == 'Pile Burning':
        return 'PILE_BURN'
    elif act == 'Piling':
        return 'PILING'
    elif act == 'Planned Treatment Burned in Wildfire':
        return 'PL_TREAT_BURNED'
    elif act == 'Precommercial Thinning (Manual)':
        return 'THIN_MAN'
    elif act == 'Precommercial Thinning (Mechanical)':
        return 'THIN_MECH'
    elif act == 'Prescribed Herbivory':
        return 'PRESCRB_HERBIVORY'
    elif act == 'Pruning':
        return 'PRUNING'
    elif act == 'Rehabilitation of Understocked Area':
        return 'REHAB_UNDRSTK_AREA'
    elif act == 'Road Clearance':
        return 'ROAD_CLEAR'
    elif act == 'Road Obliteration':
        return 'ROAD_OBLITERATION'
    elif act == 'Salvage Harvest':
        return 'SALVG_HARVEST'
    elif act == 'Sanitation Harvest':
        return 'SANI_HARVEST'
    elif act == 'Seedbed Preparation':
        return 'SEEDBED_PREP'
    elif act == 'Seed Tree Prep Step':
        return 'SEED_TREE_PREP_STEP'
    elif act == 'Seed Tree Removal Step':
        return 'SEED_TREE_REM_STEP'
    elif act == 'Seed Tree Seed Step':
        return 'SEED_TREE_SEED_STEP'
    elif act == 'Shelterwood Prep Step':
        return 'SHELTERWD_PREP_STEP'
    elif act == 'Shelterwood Removal Step':
        return 'SHELTERWD_REM_STEP'
    elif act == 'Shelterwood Seed Step':
        return 'SHELTERWD_SEED_STEP'
    elif act == 'Single Tree Selection':
        return 'SINGLE_TREE_SELECTION'
    elif act == 'Site Preparation':
        return 'SITE_PREP'
    elif act == 'Slash Disposal':
        return 'SLASH_DISPOSAL'
    elif act == 'Special Products Removal':
        return 'SP_PRODUCTS'
    elif act == 'Stream Channel Improvement':
        return 'STREAM_CHNL_IMPRV'
    elif act == 'Thinning (Manual)':
        return 'THIN_MAN'
    elif act == 'Thinning (Mechanical)':
        return 'THIN_MECH'
    elif act == 'Transition Harvest':
        return 'TRANSITION_HARVEST'
    elif act == 'Tree Planting':
        return 'TREE_PLNTING'
    elif act == 'Tree Release and Weed':
        return 'TREE_RELEASE_WEED'
    elif act == 'Tree Seeding':
        return 'TREE_SEEDING'
    elif act == 'Trees Felled (> 6in dbh)':
        return 'TREE_FELL'
    elif act == 'Variable Retention Harvest':
        return 'VARIABLE_RETEN_HARVEST'
    elif act == 'Wetland Restoration':
        return 'WETLAND_RESTOR'
    elif act == 'Wildfire Managed for Resource Benefit':
        return 'WM_RESRC_BENEFIT'
    elif act == 'Yarding/Skidding':
        return 'YARDING'
    else:
        return 'TBD'


def update_activity_cat(Cat):
    if Cat == 'Mechanical and Hand Fuels Reduction':
        return 'MECH_HFR'
    elif Cat == 'Beneficial Fire':
        return 'BENEFICIAL_FIRE'
    elif Cat == 'Grazing':
        return 'GRAZING'
    elif Cat == 'Land Protection':
        return 'LAND_PROTEC'
    elif Cat in ['Timber Harvest', 'SANI_SALVG', 'Sanitation & Salvage']:
        return 'TIMB_HARV'
    elif Cat == 'Tree Planting':
        return 'TREE_PLNTING'
    elif Cat == 'Watershed & Habitat Improvement':
        return 'WATSHD_IMPRV'
    elif Cat == 'Not Defined' or Cat in ['', ' ']:
        return 'NOT_DEFINED'
    else:
        return Cat

    
def update_broad_vegetation_type(VEG):

    if pd.isna(VEG):  # Handles pd.NA, np.nan, or None
        return np.nan  
    
    if VEG == 'Agriculture':
        return 'AGRICULTURE'
    elif VEG == 'Barren/Other' or VEG == 'Sparse':
        return 'SPARSE'
    elif VEG in ['Conifer Forest', 'Conifer Woodland', 'Forest', 'Hardwood Forest', 'Hardwood Woodland', 'Tree']:
        return 'FOREST'
    elif VEG == 'Desert Shrub':
        return 'SHRB_CHAP'
    elif VEG == 'Desert Woodland':
        return 'FOREST'
    elif VEG == 'Grass/Herbaceous' or VEG == 'Herbaceous':
        return 'GRASS_HERB'
    elif VEG == 'Shrub' or VEG == 'Shrublands and Chaparral':
        return 'SHRB_CHAP'
    elif VEG == 'Urban':
        return 'URBAN'
    elif VEG == 'Water':
        return 'WATER'
    elif VEG == 'Wetland':
        return 'WETLAND'
    elif VEG == '' or VEG == ' ':
        return np.nan
    else:
        return VEG

    
def update_bvt_userd(YN):
    if YN == 'Yes':
        return 'YES'
    elif YN == 'No':
        return 'NO'
    elif YN == '' or YN == ' ':
        return np.nan
    else:
        return YN


def update_activity_status(stat):
    if stat in ['Active', 'Active*']:
        return 'ACTIVE'
    elif stat == 'Complete':
        return 'COMPLETE'
    elif stat == 'Cancelled':
        return 'CANCELLED'
    elif stat == 'Outyear':
        return 'OUTYEAR'
    elif stat == 'Planned':
        return 'PLANNED'
    elif stat == 'Proposed':
        return 'PROPOSED'
    elif stat == '' or stat == ' ':
        return None
    else:
        return stat

    
def update_activity_uom(units):
    if units in ['acres', 'ACRES', 'Acres', 'ACRE', 'Ac']:
        return 'AC'
    elif units in ['each', 'Each', 'Ea']:
        return 'EA'
    elif units in ['Hours', 'Hour', 'Hr']:
        return 'HR'
    elif units in ['miles', 'Mi', 'mi', 'mile', 'Mile']:
        return 'MI'
    elif units == 'Other':
        return 'OTHER'
    elif units in ['Ton', 'Tons']:
        return 'TON'
    elif units == '' or units == ' ':
        return None
    else:
        return units


def update_fund_source(value):
    mapping = {
        'Greenhouse Gas Reduction Fund': 'GHG_REDUC_FUND_GGRF',
        'Proposition 68 Bond Funds': 'PROP_68_BOND_FUNDS',
        'SB 170 (2021) Wildfire Resilience Fund: General Fund': 'GENERAL_FUND_SB170_2021',
        'SB 170 (2021) Wildfire Resilience Fund: GGRF': 'GGRF_SB170_2021',
        'SB 85 (2021) Wildfire Resilience Early Action: General Fund': 'GENERAL_FUND_SB85_2021',
        'SB 85 (2021) Wildfire Resilience Early Action: GGRF': 'GGRF_SB85_2021',
        'State General Fund': 'GENERAL_FUND',
        'Other State Funds': 'OTHER_STATE_FUNDS',
        'Federal': 'FEDERAL',
        'Local': 'LOCAL',
        'Private': 'PRIVATE'
    }
    return mapping.get(value, None) if value in mapping else value


def update_residue_fate(value):
    mapping = {
        'Biochar or Other Pyrolysis': 'BIOCHAR_PYROLYSIS',
        'Broadcast Burn': 'BROADCAST_BURN',
        'Chipping': 'CHIPPING',
        'Durable Products': 'DURABLE_PRODUCTS',
        'Firewood': 'FIREWOOD',
        'Landfill': 'LANDFILL',
        'Left on Site': 'LEFT_ON_SITE',
        'Liquid Fuels': 'LIQUID_FUELS',
        'Lop and Scatter': 'LOP_SCATTER',
        'No Residue/Not Applicable': 'NO_RESIDUE/NOT_APPLICABLE',
        'Offsite Bioenergy': 'OFFSITE_BIOENERGY',
        'Other': 'OTHER',
        'Pile Burning': 'PILE_BURNING',
        'Short-Lived Products': 'SHORT-LIVED_PRODUCTS',
        'Unknown': 'UNKNOWN'
    }
    return mapping.get(value, None) if value in mapping else value


def update_units(value):
    mapping = {
        'Acres': 'AC',
        'Each': 'EA',
        'Hours': 'HR',
        'Miles': 'MI',
        'Other': 'OTHER',
        'Tons': 'TON'
    }
    return mapping.get(value, None) if value in mapping else value


def update_geom(value):
    mapping = {
        'Point': 'POINT',
        'Line': 'LINE',
        'Polygon': 'POLYGON',
        'No Shape': 'NO SHAPE'
    }
    return mapping.get(value, None) if value in mapping else value


def update_counts(value):

    # Handle missing or NA values                                                                                                                                  
    if pd.isna(value):
        return None
    
    if value == 'Yes':
        return 'YES'
    elif value == 'No':
        return 'NO'
    elif pd.isnull(value) or value == '':
        return None
    else:
        return value


def standardize_domains(gdf):

    gdf['AGENCY'] = gdf['AGENCY'].apply(update_agency_field)
    gdf['ORG_ADMIN_p'] = gdf['ORG_ADMIN_p'].apply(update_org_field)
    gdf['ORG_ADMIN_t'] = gdf['ORG_ADMIN_t'].apply(update_org_field)
    gdf['ORG_ADMIN_a'] = gdf['ORG_ADMIN_a'].apply(update_org_field)
    gdf['ADMINISTERING_ORG'] = gdf['ADMINISTERING_ORG'].apply(update_org_field)
    gdf['PROJECT_STATUS'] = gdf['PROJECT_STATUS'].apply(update_project_status_field)

    gdf['PRIMARY_FUNDING_SOURCE'] = gdf['PRIMARY_FUNDING_SOURCE'].apply(update_primary_funding_source)
    gdf['PRIMARY_FUNDING_ORG'] = gdf['PRIMARY_FUNDING_ORG'].apply(update_org_field)
    gdf['PRIMARY_OWNERSHIP_GROUP'] = gdf['PRIMARY_OWNERSHIP_GROUP'].apply(update_primary_ownership_group)

    gdf['PRIMARY_OBJECTIVE'] = gdf['PRIMARY_OBJECTIVE'].apply(update_objective)
    gdf['SECONDARY_OBJECTIVE'] = gdf['SECONDARY_OBJECTIVE'].apply(update_objective)
    gdf['TERTIARY_OBJECTIVE'] = gdf['TERTIARY_OBJECTIVE'].apply(update_objective)

    gdf['TREATMENT_STATUS'] = gdf['TREATMENT_STATUS'].apply(update_treatment_status)
    gdf['COUNTY'] = gdf['COUNTY'].apply(map_county_to_code)

    gdf['IN_WUI'] = gdf['IN_WUI'].apply(update_in_wui)
    gdf['REGION'] = gdf['REGION'].apply(update_region)

    gdf['ACTIVITY_DESCRIPTION'] = gdf['ACTIVITY_DESCRIPTION'].apply(update_activity_description)
    gdf['ACTIVITY_CAT'] = gdf['ACTIVITY_CAT'].apply(update_activity_cat)
    gdf['BROAD_VEGETATION_TYPE'] = gdf['BROAD_VEGETATION_TYPE'].apply(update_broad_vegetation_type)
    gdf['BVT_USERD'] = gdf['BVT_USERD'].apply(update_bvt_userd)

    gdf['ACTIVITY_STATUS'] = gdf['ACTIVITY_STATUS'].apply(update_activity_status)
    gdf['ACTIVITY_UOM'] = gdf['ACTIVITY_UOM'].apply(update_activity_uom)

    gdf['ADMIN_ORG_NAME'] = gdf['ADMIN_ORG_NAME'].apply(update_org_field)
    gdf['PRIMARY_FUND_ORG_NAME'] = gdf['PRIMARY_FUND_ORG_NAME'].apply(update_org_field)
    gdf['SECONDARY_FUND_SRC_NAME'] = gdf['SECONDARY_FUND_SRC_NAME'].apply(update_org_field)
    gdf['TERTIARY_FUND_ORG_NAME'] = gdf['TERTIARY_FUND_ORG_NAME'].apply(update_org_field)

    gdf['PRIMARY_FUND_SRC_NAME'] = gdf['PRIMARY_FUND_SRC_NAME'].apply(update_fund_source)
    gdf['SECONDARY_FUND_SRC_NAME'] = gdf['SECONDARY_FUND_SRC_NAME'].apply(update_fund_source)
    gdf['TERTIARY_FUND_SRC_NAME'] = gdf['TERTIARY_FUND_SRC_NAME'].apply(update_fund_source)
    gdf['RESIDUE_FATE'] = gdf['RESIDUE_FATE'].apply(update_residue_fate)
    gdf['RESIDUE_FATE_UNITS'] = gdf['RESIDUE_FATE_UNITS'].apply(update_units)
    gdf['TRMT_GEOM'] = gdf['TRMT_GEOM'].apply(update_geom)
    gdf['COUNTS_TO_MAS'] = gdf['COUNTS_TO_MAS'].apply(update_counts)
    
    
    return gdf    
