# Wildfire & Landscape Resilience Interagency Tracking System

The **Wildfire & Landscape Resilience Interagency Tracking System** (Interagency Tracking System), developed for the California Wildfire and Forest Resilience Task Force, consolidates data on wildfire and landscape resilience management activities from various federal, state, and other sources.

## Data Standardization and Enrichment Process
1. **Standardization**: Incoming data are standardized to align with the Task Force schema (version 5.2, August 2023).
2. **Enrichment**: Using reference datasets, the imported data are enhanced with additional attributes such as:
   - Vegetation cover type
   - Land ownership type
   - County
   - Task Force region
   - Wildland-Urban Interface (WUI) classification

The standardized and enriched datasets are initially stored as activity features in a flat file format. These features are then transformed into a **Project-Treatment-Activity relational database** structure.

## Technology and Outputs
The system was developed using **GeoPandas** and **GDAL**, ensuring efficient geospatial data processing. Final outputs are made available through:
- **Geodatabase Download**
- **Web Map Service**
- **Interactive Dashboard**

## Reporting Capabilities
The Interagency Tracking System produces two primary reports:
1. **Activities Report**:
   - Summarizes the level of effort (in acres) for activities performed by reporting agencies.
   - Accounts for overlapping activities, particularly those occurring across different years.

2. **Footprints Report**:
   - Eliminates overlaps by identifying the total geographic area (in acres) treated within a specific timeframe.
   - Designed to capture distinct land areas affected by treatments, such as sequential thinning and prescribed burning.

This comprehensive system enables the Task Force to monitor, analyze, and report on wildfire and landscape resilience efforts effectively.

## Installation Instructions

### Prerequisites

1. **Install GDAL**  
   Ensure that GDAL is installed on your system. You may need to refer to your system's package manager or GDAL's [official installation guide](https://gdal.org/download.html) for instructions.

2. **Install Docker**  
   Make sure Docker is installed and running on your machine. Follow the instructions on the [Docker website](https://www.docker.com/) if Docker is not already installed.

### Setup

1. **Install Required Libraries**  

   Use the `requirements.txt` file to install the necessary Python libraries:
   
   `pip install -r requirements.txt`

3. **Pull the GDAL Docker Image**

   Download the Docker image that includes the FileGDB Driver:

   `docker pull dbcawa/gdal-image`

   The downloaded Docker image contains the FileGDB Driver needed for geodatabase operations.


## Enrichment Process

### Enrich Timber Industry Spatial Data

   Edit the last five lines of the file `enrich/enrich_Timber_Industry.py` to specify the following:

   - Your `a_Reference` geodatabase path
   - The timber spatial data geodatabase path
   - The start and end years

   Then, run the following command:

   	 `python enrich/enrich_Timber_Industry.py`

### Enrich Bureau of Land Management (BLM)'s Fuels Treatments Data

   Edit the last five lines of the file `enrich/enrich_BLM.py` to specify the following:

   - Your `a_Reference` geodatabase path
   - The BLM data geodatabase path
   - The BLM feature layer name
   - The start and end years

   Then, run the following command:

   	 `python enrich/enrich_BLM.py`






