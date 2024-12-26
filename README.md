# Wildfire & Landscape Resilience Interagency Tracking System

## Overview

The **Wildfire & Landscape Resilience Interagency Tracking System** is a comprehensive data management solution developed for the California Wildfire and Forest Resilience Task Force. This system consolidates and standardizes wildfire and landscape resilience management activities data from federal, state, and other sources.

## Features

- Data standardization according to Task Force schema (v5.2, August 2023)
- Automated data enrichment with reference attributes
- Project-Treatment-Activity relational database structure
- Multiple data processing modules for different agencies
- Comprehensive reporting capabilities
- Multiple output formats including geodatabase and web services

## Prerequisites

- GDAL installation ([Official Installation Guide](https://gdal.org/download.html))
- Docker installation ([Docker Website](https://www.docker.com/))
- Python 3.x

## Installation

1. Clone the repository:
   ```bash
   git clone https://github.com/klinucsd/interagency-tracking-system
   cd interagency-tracking-system
   ```

2. Install required Python packages:
   ```bash
   pip install -r requirements.txt
   ```

3. Pull the GDAL Docker image:
   ```bash
   docker pull dbcawa/gdal-image
   ```

## Data Processing Modules

1. Data Enrichment

2. Data Appendment

3. Data Transformation

4. Activities Report

5. Footprints Report


## Data Enrichment Process

The system enriches incoming data with the following attributes:
- Vegetation cover type
- Land ownership type
- County information
- Task Force region
- Wildland-Urban Interface (WUI) classification

### Timber Industry Data Enrichment

```python
from enrich.enrich_Timber_Industry import enrich_Timber_Industry

enrich_Timber_Industry(
    input_gdb_path,      # Input geodatabase path
    input_layer_name,    # Input layer name
    reference_gdb_path,  # Reference geodatabase path
    timber_gdb_path,     # Timber spatial data path
    start_year,          # Start year for processing
    end_year,            # End year for processing
    output_gdb_path,     # Output geodatabase path
    output_layer_name    # Output layer name
)
```

Quick execution:
```bash
python enrich/enrich_Timber_Industry.py
```

### BLM Fuels Treatments Enrichment

```python
from enrich.enrich_BLM import enrich_BLM

enrich_BLM(
    input_gdb_path,      # Input geodatabase path
    input_layer_name,    # Input layer name
    reference_gdb_path,  # Reference geodatabase path
    start_year,          # Start year for processing
    end_year,            # End year for processing
    output_gdb_path,     # Output geodatabase path
    output_layer_name    # Output layer name
)
```

Quick execution:
```bash
python enrich/enrich_BLM.py
```

### NPS Fuels Treatments Enrichment

```python
from enrich.enrich_NPS import enrich_NPS_from_gdb, enrich_NPS_from_arcgis

# For geodatabase input
enrich_NPS_from_gdb(
    input_gdb_path,      # Input geodatabase path
    input_layer_name,    # Input layer name
    reference_gdb_path,  # Reference geodatabase path
    start_year,          # Start year for processing
    end_year,            # End year for processing
    output_gdb_path,     # Output geodatabase path
    output_layer_name    # Output layer name
)

# For ArcGIS Feature Service input
enrich_NPS_from_arcgis(
    feature_service_url, # ArcGIS Feature Service URL
    reference_gdb_path,  # Reference geodatabase path
    start_year,          # Start year for processing
    end_year,            # End year for processing
    output_gdb_path,     # Output geodatabase path
    output_layer_name    # Output layer name
)
```

Quick execution:
```bash
python enrich/enrich_NPS.py
```

### NFPORS Fuels Treatments Enrichment

```python
from enrich.enrich_NFPORS import enrich_NFPORS

enrich_NFPORS(
    nfpors_gdb_path,            # NFPORS geodatabase path
    nfpors_polygon_layer_name,  # NFPORS polygon layer name
    nfpors_bia_layer_name,      # BIA layer name
    nfpors_fws_layer_name,      # FWS layer name
    a_reference_gdb_path,       # Reference geodatabase path
    start_year,                 # Start year for processing
    end_year,                   # End year for processing
    output_gdb_path,            # Output geodatabase path
    output_layer_name           # Output layer name
)
```

Quick execution:
```bash
python enrich/enrich_NFPORS.py
```

### USFS Treatments Enrichment

```python
from enrich.enrich_USFS import enrich_USFS

enrich_USFS(                   
    usfs_gdb_path,             # USFS geodatabase path
    usfs_layer_name,           # USFS layer name 
    a_reference_gdb_path,      # Reference geodatabase path
    start_year,                # Start year for processing
    end_year,                  # End year for processing
    output_gdb_path,           # Output geodatabase path
    output_layer_name          # Output layer name
)
```

Quick execution:
```bash
python enrich/enrich_USFS.py
```


### Timber Industry Nonspatial Data Enrichment

```python
from enrich.enrich_Timber_Nonspatial import enrich_Timber_Nonspatial

enrich_enrich_Timber_Nonspatial(
    tn_input_excel_path,       # Path of The Timber industry nonspatial Excel file
    a_reference_gdb_path,      # Reference geodatabase path
    start_year,                # Start year for processing
    end_year,                  # End year for processing
    output_gdb_path,           # Output geodatabase path
    output_layer_name          # Output layer name
)
```

Quick execution:
```bash
python enrich/enrich_enrich_Timber_Nonspatial.py
```

### Batch Processing

The system uses YAML configuration for batch processing. Below is an example of config.yaml:

```bash
# Data processing configuration
version: '1.0'

# Global settings
global:
  reference_gdb: "a_Reference.gdb"
  date: "20241225"                     # may used as a part of the output layer name; use today if not setup
  date_format: "%Y%m%d"
  start_year: 2021
  end_year: 2023
  overwrite: false                     # overwrite the existing output layer if true; skip if false and the output layer exists 

# Data sources configuration
sources:
  blm:
    type: "geodatabase"
    input:
      gdb_path: "b_Originals/BLM_2010_2023_fromReisThomasViaUpload.gdb"
      layer_name: "BLM_2010_2023_fromReisThomasViaUpload"
    output:
      gdb_path: "/tmp/BLM_{start_year}_{end_year}.gdb"
      layer_name: "BLM_enriched_{date}"

  nfpors:
    type: "geodatabase"
    input:
      gdb_path: "b_Originals/NFPORS_2023_20240624_ServiceDownload.gdb"
      polygon_layer: "NFPORS_2023_20240619_Fuel_Treatment_Polygons_ServiceDownload"
      bia_layer: "NFPORS_2023_20240619_Current_FY_Treatments_BIA_ServiceDownload"
      fws_layer: "NFPORS_2023_20240619_Current_FY_Treatments_FWS_ServiceDownload"
    output:
      gdb_path: "/tmp/NFPORS_{start_year}_{end_year}.gdb"
      layer_name: "NFPORS_enriched_{date}"

  nps:
    type: "geodatabase"
    input:
      gdb_path: "b_Originals/New_NPS_2023_20240625_ReisThomasViaUpload_1.gdb"
      layer_name: "NPS_2023_20240625_ReisThomasViaUpload2"
    output:
      gdb_path: "/tmp/NPS_{start_year}_{end_year}.gdb"
      layer_name: "NPS_enriched_{date}"

  timber_industry_spatial:
    type: "geodatabase"
    input:
      gdb_path: "b_Originals/FFSC_MOU_2023_20240627_RebeccaFerkovichViaEmail.gdb"
      layer_name: "FFSC_MOU_IndustryOnly_Pol"
    output:
      gdb_path: "/tmp/Timber_Industry_Spatial_{start_year}_{end_year}.gdb"
      layer_name: "Timber_Industry_Spatial_{date}"

  timber_industry_nonspatial:
    type: "excel"
    input:
      excel_path: "b_Originals/Timber_Industry_Acres_2023_for_UCSD_20Sep2024.xlsx"
    output:
      gdb_path: "/tmp/Timber_Industry_Nonspatial_{start_year}_{end_year}.gdb"
      layer_name: "Timber_Industry_Nonspatial_{date}"

  usfs:
    type: "geodatabase"
    input:
      base_path: "b_Originals/USFS_FACTS_2023_20240620_uploadEmilyBrodie"
      regions: ["04", "05", "06"]
      gdb_template: "Actv_CommonAttribute_PL_Region{region}.gdb"
      layer_name: "Actv_CommonAttribute_PL"
    output:
      gdb_path: "/tmp/USFS_{start_year}_{end_year}.gdb"
      layer_name: "USFS_Region{region}_enriched_{date}"

# Processing steps configuration
processing_steps:
  - name: "enrich"
    enabled: true
  - name: "append"
    enabled: false
  - name: "transform"
    enabled: false
  - name: "report"
    enabled: false
    
```

Quick execution:
```bash
python process/ITSProcessor.py 
```


## Output Formats

1. **Geodatabase Download**
   - Complete dataset in ESRI Geodatabase format
   - Standardized schema
   - Enriched attributes

2. **Web Services**
   - ArcGIS Feature Services for accessing data
   - Interactive dashboard for data exploration

## Reporting

### Activities Report
- Quantifies effort levels (acres) by agency
- Accounts for multi-year overlapping activities
- Provides detailed activity breakdowns

### Footprints Report
- Calculates total treated geographic area
- Eliminates overlapping treatments
- Tracks sequential treatment patterns

## Technology Stack

- **GeoPandas**: Spatial data processing
- **GDAL**: Geospatial data manipulation
- **Docker**: Containerization and deployment
- **Python**: Core programming language


