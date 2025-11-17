#!/bin/bash
python enrich/enrich_BLM.py
python enrich/enrich_CalTrans.py
python enrich/enrich_CNRA.py
python enrich/enrich_IFPRS.py
python enrich/enrich_NFPORS.py
python enrich/enrich_NPS.py
python enrich/enrich_Timber_Industry.py
python enrich/enrich_Timber_Nonspatial.py
python enrich/enrich_USFS.py
python process/append.py --geom_type="polygon" # process the appended polygon before PFIRS 
python enrich/enrich_PFIRS.py

python process/append.py

python process/activity_report.py

python process/footprint.py

