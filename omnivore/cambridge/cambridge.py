import logging
from pandas import DataFrame, Series, concat
from omnivore.utils.aux import (
    extract_firstname_lastname,
    to_sf_datetime,
    toSalesforceEmail,
)

CAMBRIDGE_DATE_FORMAT = "%m/%d/%Y"

# Create a logger object
logger = logging.getLogger(__name__)

yes_no_mapper = {"Yes": True, "No": False}

quote_column_mapper = {
    "Email": "PersonEmail",
    "Customer Name: Billing Address Line 1": "Street__c",
    "Existing Heating Fuel": "Primary_Heating_Fuel__c",
    "HP Quote: Created Date": "CloseDate",
    "Quote Number": "ID_from_HPC__c",
    "Billable Time Quote": "Billable_Time_Spent__c",
    "Customer Name: Account Name": "Name",
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
    "Hot water system age": "Hot_Water_System_Age__c",
    "Existing solar": "Existing_Solar__c",
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
    "Billable Time Spent": "Billable_Time_Spent__c",
    "Customer: Account Name": "Name",
}

exclude_consulting_column = [
    "Customer: Billing Address Line 1",
    "Customer: Billing Address Line 2",
]

exclude_quote_column = [
    "Customer: Billing Address Line 1",
    "Customer Name: Billing Address Line 2",
]

# decarb_mapper = {
#     "EV / EBike technologies": "EV / EBike technologies",
#     "Heat pumps": "Heat pumps",
#     "Hot water heaters": "Hot water heaters",
#     "Household appliances": "Household appliances",
#     "Kitchen appliances": "	Kitchen appliances",
#     "Solar PV": "Solar PV",
#     "Thermal solar": "Thermal solar",
#     "Yard equipment": "Yard equipment	",
# }


def combine_notes(row: Series, column_mapper: list[str]):
    notes = []
    if (
        "Description" in row.index
        and f'{row["Description"]}' != "nan"
        and len(row["Description"]) > 0
    ):
        notes.append(f"Description:\n{row['Description']}")
    for col in row.index:
        if col not in column_mapper and f"{row[col]}" != "nan" and len(row[col]) > 0:
            notes.append(f"{col}:\n{row[col]}")
    row["Description"] = "\n".join(notes)
    return row


def cambridge_general_process(
    data: DataFrame, column_mapper: dict[str, str], no_column_inlcuded: list[str]
) -> DataFrame:
    converted = data.rename(columns=column_mapper)
    converted["PersonEmail"] = converted["PersonEmail"].apply(toSalesforceEmail)
    converted["CloseDate"] = to_sf_datetime(
        converted["CloseDate"], CAMBRIDGE_DATE_FORMAT
    )
    with_first_name = extract_firstname_lastname(converted, "Name")
    with_first_name["Name"] = (
        with_first_name["FirstName"] + " " + with_first_name["LastName"]
    )
    with_first_name["Street__c"] = with_first_name["Street__c"] + ", Cambridge, MA"
    if "Description" not in data.columns:
        data["Description"] = ""
    with_first_name = with_first_name.apply(
        combine_notes,
        axis=1,
        args=(
            list(column_mapper.keys())
            + list(column_mapper.values())
            + no_column_inlcuded,
        ),
    )

    return with_first_name


def cambridge(
    consulting_data: DataFrame, quote_data: DataFrame
) -> tuple[DataFrame, DataFrame]:
    consultation = cambridge_general_process(
        consulting_data,
        consulting_column_mapper,
        exclude_consulting_column,
    )
    consultation = consultation.reset_index(drop=True)
    quote = cambridge_general_process(
        quote_data,
        quote_column_mapper,
        exclude_quote_column,
    )
    quote = quote.reset_index(drop=True)
    combined = concat(
        [consultation, quote],
    )
    combined["Existing_Solar__c"] = combined["Existing_Solar__c"].map(yes_no_mapper)
    combined["Existing_EV_Charging__c"] = combined["Existing_EV_Charging__c"].map(
        yes_no_mapper
    )
    combined["StageName"] = "Not Yet Scheduled"
    combined["Number_of_Units_in_the_Building_Condo_As__c"] = (
        combined["Number_of_Units_in_the_Building_Condo_As__c"]
        .fillna("")
        .str.replace("0", "")
    )
    return combined.reset_index(drop=True)
