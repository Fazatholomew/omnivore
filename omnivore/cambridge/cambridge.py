import logging
from pandas import DataFrame
from omnivore.utils.aux import (
    extract_firstname_lastname,
    to_sf_datetime,
    toSalesforceEmail,
)

CAMBRIDGE_DATE_FORMAT = "%m/%d/%Y"

# Create a logger object
logger = logging.getLogger(__name__)

quote_column_mapper = {
    "Email": "PersonEmail",
    "Customer Name: Billing Address Line 1": "Street__c",
    "Existing Heating Fuel": "Heating_Fuel__c",
    "Customer Notes from Submission": "Description",
}

consulting_column_mapper = {
    "Survey ID": "ID_from_HPC__c",
    "Customer email": "PersonEmail",
    "Contact Mailing Address": "Street__c",
    "Number of units in building": "Number_of_Units_in_the_Building_Condo_As__c",
    "Number of floors in building": "Number_of_Floors__c",
    "Square footage": "Square_Footage__c",
    "Building age": "Building_Age__c",
    "Current heating fuel": "Primary_Heating_Fuel__c",
    "Heating system age": "Heating_System_Age__c",
    "Cooling system age": "Cooling_System_Age__c",
    "Hot water fuel": "Hot_Water_Fuel__c",
    "Hot water system type": "Hot_Water_System_Type__c",
    "Hot water system age": "Existing_Solar__c",
    "Roof age": "Roof_Age__c",
    "Electrical panel capacity": "Electrical_Panel_Capacity__c",
    "Number of spaces": "Number_of_Spaces__c",
    "Existing EV charging": "Existing_EV_Charging__c",
    "Decarb technologies of interest": "Which_decarb_technologies_interest_you__c",
    "Date of Communication": "CloseDate",
    "Map/Lot ID (Property Records)": "Map_Lot_ID_Cambridge_Property_Assessors__c",
    "Building Envelope": "Building_Envelope__c",
    "Relationship to building (for 5+ units)": "Relationship_to_Building__c",
    "Building Owner/Property Management Email": "Owner_Property_Management_Email__c",
    "Building Owner/Property Management Name": "Owner_Property_Management_Name__c",
    "Building Owner/Property Management Phone": "Owner_Property_Management_Phone__c",
    "Cambridge - found consultation helpful?": "Did_you_find_your_consultation_helpful__c",
    "Cambridge - advisor answered questions?": "Were_your_advisors_helpful__c",
    "Cambridge - consultation feedback?": "Open_feedback_what_should_we_know__c",
}


def cambridge_general_process(
    data: DataFrame, column_mapper: dict[str, str]
) -> DataFrame:
    converted = data.rename(columns=column_mapper)
    converted["PersonEmail"] = converted["PersonEmail"].apply(toSalesforceEmail)
    converted["CloseDate"] = to_sf_datetime(
        converted["CloseDate"], CAMBRIDGE_DATE_FORMAT
    )
    with_first_name = extract_firstname_lastname(converted, "Customer: Account Name")
    with_first_name["Name"] = (
        with_first_name["FirstName"] + " " + with_first_name["LastName"]
    )
    with_first_name["Street__c"] = with_first_name["Street__c"] + ", Cambridge, MA"
    return with_first_name


def cambridge(
    consulting_data: DataFrame, quote_data: DataFrame
) -> tuple[DataFrame, DataFrame]:
    return cambridge_general_process(consulting_data, consulting_column_mapper)
    # cambridge_general_process(quote_data, quote_column_mapper),
