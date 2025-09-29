import arcpy
import os

years = ['2021', '2022', '2023', '2024']
temp_path = 'temp_mns.gdb'

for y in years:
    Spaghetti_FeatureToPolygon = os.path.join(temp_path, "Spaghetti_FeatureToPolygon"+y)

    arcpy.management.FeatureToPolygon(
        in_features=[os.path.join(temp_path, "spaghetti"+y)], 
        out_feature_class=Spaghetti_FeatureToPolygon
    )
    # check results
    result = arcpy.management.GetCount(Spaghetti_FeatureToPolygon)
    print("{} has {} records".format(Spaghetti_FeatureToPolygon, result[0]))