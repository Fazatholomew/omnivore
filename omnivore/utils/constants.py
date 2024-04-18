# Main Spreadsheet ID
SPREADSHEET_ID = "1DzZ1nQLiVb0LpXbi8E_T4KsOg2uIYR5irtgt5gNWyuc"
# Neeeco A1Notation Data
NEEECO_A1 = "Neeeco!A2:AH"
# Neeeco A1Notation Weatherization Data
NEEECOWX_A1 = "Neeeco Wx!A2:I"
# NEEECO A1Notation where data from previous run is stored;
PREVIOUS_A1 = "Processed Rows!A1:A"
# Person Account Record Type ID
PERSON_ACCOUNT_ID = "0123i000000YGesAAG"
# CFP Person Account Record Type ID
CFP_ACCOUNT_ID = "0128Z0000002307QAA"
# Home Energy Assessment Record Type ID
HEA_ID = "0123i000000YF7QAAW"
# Renters Record Type ID
RENTER_ID = "0123i000000YiBoAAK"
# Community Solar ID
CS_ID = "0123i000000YnfmAAC"
# Energy Bill Checkup ID
ENERGY_ID = "0123i000000Ys96AAC"
# CFP ID
CFP_OPP_ID = "0128Z000000230CQAQ"
# Cambridge Record Type ID
CAMBRIDGE_OPP_ID = "0128Z00000024eOQAQ"
# Neeeco Account ID
NEEECO_ACCID = "0013i00000AtGAvAAN"
# Revise Account ID
REVISE_ACCID = "0013i00000AtGAqAAN"
# Homeworks Account ID
HOMEWORKS_ACCID = "0013i00000AtGGeAAN"
# VHI Account ID
VHI_ACCID = "0013i00001600C3AAI"
# Cambridge Account ID
CAMBRIDGE_ACCID = "0018Z000038PTjHQAW"
# AIE ID seperator
AIE_ID_SEPARATOR = "<|>"
# Opps Record Type IDs
OPPS_RECORD_TYPE = "', '".join([HEA_ID, RENTER_ID, CS_ID, ENERGY_ID, CFP_OPP_ID])
# Unit number extension
UNIT_NAMES = ["apt", "unit", "#"]
# Address street extension
STREET_NAMES = [
    "street",
    "st",
    "road",
    "rd",
    "avenue",
    "ave",
    "drive",
    "dr",
    "circle",
    "cr",
]
# Staff Name to Staff Salesforce Account ID
STAFF_SF_ACCOUNTS = {
    "Julia Soriano": "0053i000002vBIU",
    "Grace Umana": "0053i0000025kVv",
    "Mamadou Balde": "0053i0000025rzW",
    "Jon Simning": "0053i0000026jNC",
}
# Staff Name to Staff Account ID
STAFF_ACCOUNTS = {
    "Jared Johnson": "0013i00000IdX6V",
    "Grace Umana": "0013i00000IeGmd",
    "Serra Kilic": "0013i00000IdXru",
    "Mamadou Balde": "0013i00000Ie0IF",
    "Jon Simning": "0013i00000IeGmx",
    "Julia Soriano": "0013i00001Jd1xw",
    "Jean Gouvea": "0013i0000238Kq2AAE",
}

# Opportunity columns to fetch
OPPORTUNITY_COLUMNS = [
    "Id",
    "Street__c",
    "Unit__c",
    "City__c",
    "State__c",
    "Zipcode__c",
    "Name",
    "HEA_Date_And_Time__c",
    "CloseDate",
    "StageName",
    "Health_Safety_Barrier_Status__c",
    "Health_Safety_Barrier__c",
    "isVHEA__c",
    "Weatherization_Status__c",
    "Weatherization_Date_Time__c",
    "Contract_Amount__c",
    "Final_Contract_Amount__c",
    "AccountId",
    "RecordTypeId",
    "ID_from_HPC__c",
    "Owner_Renter__c",
    "All_In_Energy_ID__c",
    "Staff_acc__c",
    "Don_t_Omnivore__c",
    "Set_By__c",
    "Cancelation_Reason_s__c",
    "Legacy_Post_Assessment_Notes__c",
    "HPC__c",
    "CampaignId",
    "Primary_Heating_Fuel__c",
    "Description",
    "Number_of_Units_in_the_Building_Condo_As__c",
]

CAMBRIDGE_OPPORTUNITY_COLUMNS = [
    "Number_of_Floors__c",
    "Square_Footage__c",
    "Building_Age__c",
    "Heating_System_Age__c",
    "Cooling_System_Age__c",
    "Hot_Water_Fuel__c",
    "Hot_Water_System_Type__c",
    "Existing_Solar__c",
    "Roof_Age__c",
    "Electrical_Panel_Capacity__c",
    "Number_of_Spaces__c",
    "Existing_EV_Charging__c",
    "Which_decarb_technologies_interest_you__c",
    "Map_Lot_ID_Cambridge_Property_Assessors__c",
    "Building_Envelope__c",
    "Relationship_to_Building__c",
    "Owner_Property_Management_Email__c",
    "Owner_Property_Management_Name__c",
    "Owner_Property_Management_Phone__c",
    "Did_you_find_your_consultation_helpful__c",
    "Were_your_advisors_helpful__c",
    "Open_feedback_what_should_we_know__c",
    "Billable_Time_Spent__c",
    "What_problem_s_are_you_facing_cambridge__c",
    "Consultation_Date__c",
    "What_is_known_about_cambridge__c",
    "Building_common_areas_or_cambridge__c",
    "Location_IOM__c",
    "Ventilation_system_for_indoor_cambridge__c",
    "Site_Visit_Date_date__c",
    "Milestone_1_Date_cambridge__c",
    "Milestone_2_Date_cambridge__c",
    "Milestone_3_Date_cambridge__c",
    "Milestone_4_Date_cambridge__c",
    "Expected_Follow_Up_Date__c",
    "Total_Square_Footage__c",
    "Cambridge_Data_Sorce__c",
    "Site_Visit_cambridge__c",
    "Building_size_cambridge__c",
]
# Account columns to fetch
ACCOUNT_COLUMNS = [
    "Id",
    "BillingStreet",
    "BillingCity",
    "BillingState",
    "BillingPostalCode",
    "FirstName",
    "LastName",
    "Phone",
    "PersonEmail",
    "Gas_Utility__c",
    "Electric_Utility__c",
    "RecordTypeId",
    "Owner_Renter__c",
    "All_In_Energy_ID__c",
    "Field_Staff__c",
    "Prefered_Lan__c",
    "PersonDoNotCall",
]

# CFP Town Dict

CFP_TOWS = {
    "williamstown": "7018Z0000033D6u",
    "boston": "7013i000000i5vRAAQ",
    "pittsfield": "7013i000000i5vWAAQ",
    "springfield": "7013i000000i5vWAAQ",
    "west springfield": "7013i000000i5vWAAQ",
    "framingham": "7013i000000i5vbAAA",
    "natick": "7013i000000i5vbAAA",
    "gloucester": "7013i000000i5vgAAA",
    "andover": "7013i000000i5vNAAQ",
    "north andover": "7013i000000i5vNAAQ",
    "lawrence": "7013i000000i5vNAAQ",
    "haverhill": "7013i000000i5vNAAQ",
    "methuen": "7013i000000i5vNAAQ",
    "lowell": "7013i000000i5vlAAA",
    "malden": "7013i000000i5vXAAQ",
    "melrose": "7013i000000i5vYAAQ",
    "chelsea": "7013i000000i5vqAAA",
    "revere": "7013i000000i5vqAAA",
    "winthrop": "7013i000000i5vqAAA",
    "norwood": "7013i000000i5vrAAA",
    "sharon": "7013i000000i5vrAAA",
    "walpole": "7013i000000i5vrAAA",
    "salem": "7013i000000i5vOAAQ",
    "beverly": "7013i000000i5vOAAQ",
    "shelburne falls": "7013i000000i5vPAAQ",
    "shelburne": "7013i000000i5vPAAQ",
    "wareham": "7013i000000i5vsAAA",
    "westborough": "7013i000000i5vmAAA",
    "quincy": "7013i000000i5vZAAQ",
    "randolph": "7013i000000i5vZAAQ",
    "cambridge": "7013i000000i5vCAAQ",
    "great barrington": "7018Z0000033D6u",
    "lee": "7018Z0000033D6u",
    "lanesborough": "7018Z0000033D6u",
    "adams": "7018Z0000033D6u",
    "north adams": "7018Z0000033D6u",
    "arlington": "7018Z0000033D6x",
    "brockton": "7018Z0000033D6v",
    "attleboro": "7018Z0000033D6w",
    "taunton": "7018Z0000033D6w",
    "fall river": "7018Z0000033D6w",
    "new bedford": "7018Z0000033DpwQAE",
    "westport": "7018Z0000033DpwQAE",
    "everett": "7018Z0000033D6y",
    "lynn": "7018Z0000033D6z",
    "swampscott": "7018Z0000033D6z",
    "aquinnah": "7018Z0000026uf6QAA",
    "chilmark": "7018Z0000026uf6QAA",
    "edgartown": "7018Z0000026uf6QAA",
    "oak bluffs": "7018Z0000026uf6QAA",
    "tisbury": "7018Z0000026uf6QAA",
    "west tisbury": "7018Z0000026uf6QAA",
    "watertown": "7018Z0000033D71",
}
# Salesforce Format
DATETIME_SALESFORCE = "%Y-%m-%dT%H:%M:%S.000-07:00"

# HPC .env data URL
HPC_DATA_URLS = ["NEEECO_DATA_URL", "NEEECO_WX_DATA_URL"]
