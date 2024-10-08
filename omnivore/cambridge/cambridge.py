from pandas import DataFrame, Series, concat, isna
from omnivore.utils.aux import (
    extract_firstname_lastname,
    to_sf_datetime,
    toSalesforceEmail,
    DATETIME_SALESFORCE,
)
from datetime import datetime
from re import findall
from omnivore.utils.logging import getLogger

CAMBRIDGE_DATE_FORMAT = "%m/%d/%Y"

# Create a logger object

logger = getLogger(__name__)

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
    "Number of units in building": "Building_size_cambridge__c",
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
    "Cambridge - advisor answered questions?": "Did_they_answer_all_your_questions__c",
    "Cambridge - consultation feedback?": "Open_feedback_what_should_we_know__c",
    "Billable Time Spent": "Billable_Time_Spent__c",
    "Customer: Account Name": "Name",
    "Current heating system type": "Current_heating_system_type__c",
    "Existing cooling system": "Existing_cooling_system__c",
    "Customer primary motivation": "Customer_primary_motivation__c",
}

relationship_mapper = {
    "Building Owner": "Building owner",
    "Building owner": "Building owner",
    "Property Manager": "Property Manager",
    "Trustee / Board Member": "Trustee / Board Member",
    "Trustee/Board Member": "Trustee / Board Member",
    "Unit Owner": "Unit owner",
    "Unit owner": "Unit owner",
}

new_ecology_column_mapper = {
    "ID": "ID_from_HPC__c",
    "Notes": "Description",
    # "Status": "StageName",
    "Billable Time Spent": "Billable_Time_Spent__c",
    "Timestamp": "CloseDate",
    "Name": "Name",
    "Your email": "PersonEmail",
    "Contact Mailing Address": "Street__c",
    "Current heating fuel": "Primary_Heating_Fuel__c",
    "What problem(s) are you facing?": "What_problem_s_are_you_facing_cambridge__c",
    "Have you completed a Mass Save Energy Audit in the past 5 years? ": "Previous_Mass_Save_assessment__c",
    "What decarb technologies are you interested in pursuing? ": "Which_decarb_technologies_interest_you__c",
    "Consultation Date": "Consultation_Date__c",
    "Map Lot ID Cambridge Property Assessors database:": "Map_Lot_ID_Cambridge_Property_Assessors__c",
    "Number of units": "Building_size_cambridge__c",
    "Number of floors": "Number_of_Floors__c",
    "Building Envelope:": "Building_Envelope__c",
    "Building Age": "Building_Age__c",
    "Heating system age": "Heating_System_Age__c",
    "Hot water fuel": "Hot_Water_Fuel__c",
    "Hot water system type": "Hot_Water_System_Type__c",
    "Hot water system age": "Hot_Water_System_Age__c",
    "Existing solar": "Existing_Solar__c",
    "Roof age": "Roof_Age__c",
    "Electrical panel capacity": "Electrical_Panel_Capacity__c",
    "What is known about electrical service to building and to individual units?": "What_is_known_about_cambridge__c",
    "Building common areas or non-residential uses: Separate mechanicals? Who pays?": "Building_common_areas_or_cambridge__c",
    "Number of Parking spaces: (Number)": "Number_of_Spaces__c",
    "Location": "Location_IOM__c",
    "Ventilation system for indoor parking?": "Ventilation_system_for_indoor_cambridge__c",
    "Existing EV charging": "Existing_EV_Charging__c",
    "Client Relationship to building:": "Relationship_to_Building__c",
    "Building Owner / Property Management Email:": "Owner_Property_Management_Email__c",
    "Building Owner / Property Management Phone:": "Owner_Property_Management_Phone__c",
    "Building Owner / Property Management Name:": "Owner_Property_Management_Name__c",
    "Expected Follow Up Date: ": "Expected_Follow_Up_Date__c",
    "Site Visit Date": "Site_Visit_Date_date__c",
    "Site Visit": "Site_Visit_cambridge__c",
    "Action (Multi-Select Picklist)": "Cambridge_Decarb_Action__c",  # Cambridge Action
    "Milestone 1 Date Estimate": "Milestone_1_Date_cambridge__c",
    "Milestone 2 Date": "Milestone_2_Date_cambridge__c",
    "Milestone 3 Date": "Milestone_3_Date_cambridge__c",
    "Milestone 4 Date": "Milestone_4_Date_cambridge__c",
}

exclude_consulting_column = [
    "Customer: Billing Address Line 1",
    "Customer: Billing Address Line 2",
]

exclude_quote_column = [
    "Customer: Billing Address Line 1",
    "Customer Name: Billing Address Line 2",
]

stage_mapper = {
    "Unknown - needs HEA": "Prospecting",
    "No - but plan to ": "Prospecting",
    "No - weatherization barriers ": "Health & Safety Barrier",
    "Yes - all recommendations completed  ": "Completed",
    "Yes - some recommendations completed": "Completed",
    "No - no interest": "Canceled/No Longer Interested",
    "No - but plan to": "Not Yet Scheduled",
    "No - weatherization barriers": "Health & Safety Barrier",
    "Yes - all recommendations completed": "Completed",
    "Yes - some recommendations completed  ": "Completed",
}

wx_mapper = {
    "Unknown - needs HEA": "Not Yet Scheduled",
    "No - but plan to ": "Not Yet Scheduled",
    "No - weatherization barriers ": "Not Yet Scheduled",
    "Yes - all recommendations completed  ": "Completed",
    "Yes - some recommendations completed": "Completed",
    "No - no interest": "Not Yet Scheduled",
    "No - but plan to": "Not Yet Scheduled",
    "No - weatherization barriers": "Not Yet Scheduled",
    "Yes - all recommendations completed": "Completed",
}

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

number_ranges = [
    "2-4",
    "5-10",
    "11-25",
    "26-50",
]

"""
Site Visit
Requested
Scheduled
Complete
Not Needed
Status
Our Court
Complete
Terminated
Client Court
Repeat Client
"""

building_age_ranges = ["1700-1799", "1800-1899", "1900-1950", "1951-1999"]


def determine_building_age_range(number: str) -> str:
    if isna(number) or number.lower() == "nan":
        return ""
    numbers = findall(r"\d+", number)
    if len(numbers) == 0:
        return ""
    current_number = int(numbers[0])
    if current_number > 1999:
        return "2000+"
    if current_number < 1700:
        return "pre-1700"
    for ranges in building_age_ranges:
        lower_bound, upper_bound = ranges.split("-")
        if int(lower_bound) <= current_number and int(upper_bound) >= current_number:
            return ranges
    return ""


def determine_number_range(number: str) -> str:
    if isna(number) or number.lower() == "nan":
        return "1"
    numbers = findall(r"\d+", number)
    if len(numbers) == 0:
        return "1"
    current_number = int(numbers[0])
    if current_number < 1:
        return "1"
    if current_number > 50:
        return "50+"
    for ranges in number_ranges:
        lower_bound, upper_bound = ranges.split("-")
        if int(lower_bound) <= current_number and int(upper_bound) >= current_number:
            return ranges
    return "1"


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
    data: DataFrame,
    column_mapper: dict[str, str],
    no_column_inlcuded: list[str] = [],
    date_format: str = CAMBRIDGE_DATE_FORMAT,
) -> DataFrame:
    converted = data.rename(columns=column_mapper)
    if "Description" not in converted.columns:
        converted["Description"] = ""
    converted = converted.apply(
        combine_notes,
        axis=1,
        args=(
            list(column_mapper.keys())
            + list(column_mapper.values())
            + no_column_inlcuded,
        ),
    )
    converted["PersonEmail"] = converted["PersonEmail"].apply(toSalesforceEmail)
    converted["CloseDate"] = to_sf_datetime(converted["CloseDate"], date_format)
    converted["CloseDate"] = converted["CloseDate"].fillna(
        datetime.now().strftime(DATETIME_SALESFORCE)
    )
    with_first_name = extract_firstname_lastname(converted, "Name")
    with_first_name["Name"] = (
        with_first_name["FirstName"] + " " + with_first_name["LastName"]
    )
    with_first_name["Street__c"] = (
        with_first_name["Street__c"].fillna("") + ", Cambridge, MA"
    )

    return with_first_name


def new_ecology(_data: DataFrame) -> DataFrame:
    data = _data[~_data["ID"].isna()].copy()
    for column in [
        "Consultation Date",
        "Milestone 1 Date Estimate",
        "Milestone 2 Date",
        "Milestone 3 Date",
        "Milestone 4 Date",
        "Site Visit Date",
        "Expected Follow Up Date: ",
    ]:
        data[column] = to_sf_datetime(data[column], CAMBRIDGE_DATE_FORMAT)
    data["Action (Multi-Select Picklist)"] = (
        data["Action (Multi-Select Picklist)"]
        .str.replace(
            "Recommended decarbonization actions and next steps",
            "Recommended decarbonization actions",
        )
        .str.replace(", ", ";")
        .str.replace(",", ";")
    )
    no_need_hea_mask = data["Weatherized:"].isna() & (
        data["Have you completed a Mass Save Energy Audit in the past 5 years? "]
        == "No"
    )
    data.loc[no_need_hea_mask, "Weatherized:"] = "Unknown - needs HEA"

    data["StageName"] = data["Weatherized:"]
    return data


def cambridge(
    consulting_data: DataFrame, quote_data: DataFrame, _new_ecology_data: DataFrame
) -> DataFrame:
    new_ecology_data = new_ecology(_new_ecology_data)
    processed = []
    for current_data in [
        {
            "data": consulting_data,
            "column_mapper": consulting_column_mapper,
            "exclude_column": exclude_consulting_column,
            "type": "Consultation",
            "stage_column": "Home Weatherized?",
            "stage_mapper": stage_mapper,
            "wx_column": "Home Weatherized?",
            "wx_mapper": wx_mapper,
        },
        {
            "data": quote_data,
            "column_mapper": quote_column_mapper,
            "exclude_column": exclude_quote_column,
            "type": "Quote",
        },
        {
            "data": new_ecology_data,
            "column_mapper": new_ecology_column_mapper,
            "date_format": "%m/%d/%Y %H:%M:%S",
            "type": "New Ecology",
            "stage_mapper": stage_mapper,
            "stage_column": "Weatherized:",
            "wx_column": "Weatherized:",
            "wx_mapper": wx_mapper,
        },
    ]:
        result = cambridge_general_process(
            current_data["data"],
            current_data["column_mapper"],
            current_data["exclude_column"] if "exclude_column" in current_data else [],
            (
                current_data["date_format"]
                if "date_format" in current_data
                else CAMBRIDGE_DATE_FORMAT
            ),
        )
        result["Cambridge_Data_Sorce__c"] = current_data["type"]
        if "stage_mapper" in current_data and "stage_column" in current_data:
            result["StageName"] = result[current_data["stage_column"]].map(
                current_data["stage_mapper"]
            )
        else:
            result["StageName"] = "Not Yet Scheduled"

        if "wx_column" in current_data and "wx_mapper" in current_data:
            result["Weatherization_Status__c"] = result[current_data["wx_column"]].map(
                current_data["wx_mapper"]
            )
        else:
            result["Weatherization_Status__c"] = "Not Yet Scheduled"
        result = result.reset_index(drop=True)
        if len(result) > 0:
            processed.append(result)

    if len(processed) == 0:
        return DataFrame([])

    if len(processed) == 1:
        combined = processed[0]
    else:
        combined = concat(processed, ignore_index=True)

    combined["Existing_Solar__c"] = combined["Existing_Solar__c"].map(yes_no_mapper)
    combined["Existing_EV_Charging__c"] = combined["Existing_EV_Charging__c"].map(
        yes_no_mapper
    )
    combined["Building_size_cambridge__c"] = (
        combined["Building_size_cambridge__c"].fillna("1").str.replace("0", "1")
    )
    combined["Which_decarb_technologies_interest_you__c"] = (
        combined["Which_decarb_technologies_interest_you__c"]
        .fillna("")
        .str.replace(", ", ";")
        .str.replace(",", ";")
    )
    for current_column in [
        "Cooling_System_Age__c",
        "Heating_System_Age__c",
        "Hot_Water_System_Age__c",
        "Roof_Age__c",
        "Total_Square_Footage__c",
        "Number_of_Spaces__c",
    ]:
        if current_column in combined:
            combined[current_column] = (
                combined[current_column].fillna("").astype(str).str.extract(r"(\d+)")
            )
    no_last_name_mask = (combined["LastName"].isna() | (combined["LastName"] == "")) & (
        ~combined["FirstName"].isna() & (combined["FirstName"] != "")
    )
    combined.loc[no_last_name_mask, "LastName"] = combined.loc[no_last_name_mask][
        "FirstName"
    ]
    combined.loc[no_last_name_mask, "FirstName"] = ""
    no_name_mask = combined["LastName"].isna() | (combined["LastName"] == "")
    combined.loc[no_name_mask, "LastName"] = "Unknown"
    mask = combined["Name"].isna() | (combined["Name"] == "")
    combined.loc[mask, "Name"] = combined["FirstName"] + " " + combined["LastName"]
    combined["Number_of_Units_in_the_Building_Condo_As__c"] = combined[
        "Building_size_cambridge__c"
    ].apply(determine_number_range)
    combined["Building_Age__c"] = combined["Building_Age__c"].apply(
        determine_building_age_range
    )
    combined["Relationship_to_Building__c"] = combined[
        "Relationship_to_Building__c"
    ].map(relationship_mapper)
    combined["StageName"] = combined["StageName"].fillna("Not Yet Scheduled")
    return combined.reset_index(drop=True)
