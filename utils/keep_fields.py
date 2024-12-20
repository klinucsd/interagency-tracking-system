
import logging
from its_logging.logger_config import logger

logger = logging.getLogger('utils.keep_fields')


def keep_fields(keep_table):
    
    # List of fields to keep
    fields_to_keep = [
        "PROJECTID_USER", "AGENCY", "ORG_ADMIN_p", "PROJECT_CONTACT", "PROJECT_EMAIL",
        "ADMINISTERING_ORG", "PROJECT_NAME", "PROJECT_STATUS", "PROJECT_START", 
        "PROJECT_END", "PRIMARY_FUNDING_SOURCE", "PRIMARY_FUNDING_ORG", "IMPLEMENTING_ORG",
        "LATITUDE", "LONGITUDE", "Val_Status_p", "Val_Message_p", "Val_RunDate_p",
        "Review_Status_p", "Review_Message_p", "Review_RunDate_p", "Dataload_Status_p",
        "Dataload_Msg_p", "TRMTID_USER", "PROJECTID", "PROJECTNAME_", "ORG_ADMIN_t",
        "PRIMARY_OWNERSHIP_GROUP", "PRIMARY_OBJECTIVE", "SECONDARY_OBJECTIVE",
        "TERTIARY_OBJECTIVE", "TREATMENT_STATUS", "COUNTY", "IN_WUI", "REGION",
        "TREATMENT_AREA", "TREATMENT_START", "TREATMENT_END", "RETREATMENT_DATE_EST",
        "TREATMENT_NAME", "Val_Status_t", "Val_Message_t", "Val_RunDate_t",
        "Review_Status_t", "Review_Message_t", "Review_RunDate_t", "Dataload_Status_t",
        "Dataload_Msg_t", "ACTIVID_USER", "TREATMENTID_", "ORG_ADMIN_a",
        "ACTIVITY_DESCRIPTION", "ACTIVITY_CAT", "BROAD_VEGETATION_TYPE", "BVT_USERD",
        "ACTIVITY_STATUS", "ACTIVITY_QUANTITY", "ACTIVITY_UOM", "ACTIVITY_START",
        "ACTIVITY_END", "ADMIN_ORG_NAME", "IMPLEM_ORG_NAME", "PRIMARY_FUND_SRC_NAME",
        "PRIMARY_FUND_ORG_NAME", "SECONDARY_FUND_SRC_NAME", "SECONDARY_FUND_ORG_NAME",
        "TERTIARY_FUND_SRC_NAME", "TERTIARY_FUND_ORG_NAME", "ACTIVITY_PRCT",
        "RESIDUE_FATE", "RESIDUE_FATE_QUANTITY", "RESIDUE_FATE_UNITS", "ACTIVITY_NAME",
        "VAL_STATUS_a", "VAL_MSG_a", "VAL_RUNDATE_a", "REVIEW_STATUS_a", "REVIEW_MSG_a",
        "REVIEW_RUNDATE_a", "DATALOAD_STATUS_a", "DATALOAD_MSG_a", "Source", "Year",
        "Year_txt", "Act_Code", "Crosswalk", "Federal_FY", "State_FY", "TRMT_GEOM",
        "COUNTS_TO_MAS"
    ]
    
    # Make sure to include geometry column if it exists
    if 'geometry' in keep_table.columns:
        fields_to_keep.append('geometry')
    
    # Keep only the specified fields
    keep_table = keep_table[fields_to_keep]
    
    return keep_table
