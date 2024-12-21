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

## Data Enrichment Process

The system enriches incoming data with the following attributes:
- Vegetation cover type
- Land ownership type
- County information
- Task Force region
- Wildland-Urban Interface (WUI) classification

## Output Formats

1. **Geodatabase Download**
   - Complete dataset in ESRI Geodatabase format
   - Standardized schema
   - Enriched attributes

2. **Web Services**
   - Web Map Service (WMS) for visualization
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