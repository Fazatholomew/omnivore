from omnivore.utils.aux import to_sf_datetime, toSalesforceEmail, toSalesforcePhone
from pandas import DataFrame

import logging

REVISE_DATE_FORMAT = "%m/%d/%Y"

# Create a logger object
logger = logging.getLogger(__name__)


owner_renter_mapper = {"Owner": "Owner", "Landlord": "Owner", "Renter": "Owner"}

hs_mapper = {
    "Asbestos": "Asbestos",
    "CST": "Combustion Safety Failure",
    "K&T ? Major": "Knob & Tube (Major)",
    "K&T ? Minor": "Knob & Tube (Minor)",
    "Mold": "Mold/Moisture",
    "Untreatable Dirt Floor": "Dirt Floor Basement",
    "Unvented Kitchen Fan": "Unvented Bath Fan",
    "Vermiculite": "Vermiculite",
}

stage_mapper = {
    "WX Unsigned": "Recommended - Unsigned",
    "No Opportunity": "No Opportunity",
    "Scheduled": "Signed Contracts",
    "Health and Safety": "Health & Safety Barrier",
    "WX Signed": "Signed Contracts",
    "Scheduled A/S Only": "Recommended - Unsigned",
    "Opportunity Identified": "Canceled",
}

column_mapper = {
    "Company / Account": "ID_from_HPC__c",
    "First Name": "FirstName",
    "Last Name": "LastName",
    "Landlord / Owner / Renter": "Owner_Renter__c",
    "Email": "PersonEmail",
    "Street": "Street__c",
    "City": "City__c",
    "Zip/Postal Code": "Zipcode__c",
    "Created Date": "CloseDate",
    "Desired Date of Audit": "Original_HEA_Date_Time__c",
    "Audit Date": "HEA_Date_And_Time__c",
    "WX Install Date from Event": "Weatherization_Date_Time__c",
    "Total Contract Amount (Pre-Rebate)": "Contract_Amount__c",
    "Health and Safety Status": "Health_Safety_Barrier__c",
    "Visit Result": "StageName",
    "Language": "Prefered_Lan__c",
}

wx_column_mapper = {
    "Account Name": "ID_from_HPC__c",
    "First Name": "FirstName",
    "Last Name": "LastName",
    "Email": "PersonEmail",
    "Billing Street": "Street__c",
    "Billing City": "City__c",
    "Billing Zip/Postal Code": "Zipcode__c",
    "WX Install Date": "Weatherization_Date_Time__c",
    "Language": "Prefered_Lan__c",
}

language_mapper = {
    "Spanish Required": "Spanish",
    "English": "English",
    "Spanish Preferred": "Spanish",
    "Portuguese Required": "Portuguese",
    "Language": "Other",
    "French": "French",
    "Other - Please Note Below": "Other",
}


def merge_columns(
    data: DataFrame, columns: list[str], primary="x", secondary="y"
) -> DataFrame:
    for column in columns:
        primary_column = f"{column}_{primary}"
        secondary_column = f"{column}_{secondary}"
        if primary_column not in data.columns and secondary_column not in data.columns:
            continue
        data[column] = data[primary_column].combine_first(data[secondary_column])
    return data


def merge_file_revise(hea: DataFrame, wx: DataFrame) -> DataFrame:
    hea_renamed = hea.rename(columns=column_mapper)
    wx_renamed = wx.rename(columns=wx_column_mapper)
    combined = hea_renamed.rename(columns=column_mapper).merge(
        wx_renamed, how="outer", on="ID_from_HPC__c"
    )
    merged = merge_columns(
        combined,
        [
            "FirstName",
            "LastName",
            "PersonEmail",
            "Phone",
            "Street__c",
            "City__c",
            "Zipcode__c",
            "Weatherization_Date_Time__c",
            "Prefered_Lan__c",
        ],
        "y",
        "x",
    )
    return merged


def revise(data: DataFrame) -> DataFrame:
    merged = data.copy()
    merged["Health_Safety_Barrier__c"] = merged["Health_Safety_Barrier__c"].fillna("")
    merged["Health_Safety_Barrier__c"] = (
        merged["Health_Safety_Barrier__c"]
        .str.split("; ")
        .apply(
            lambda x: [
                hs_mapper[current_hs] for current_hs in x if current_hs in hs_mapper
            ]
        )
    )
    merged["Owner_Renter__c"] = merged["Owner_Renter__c"].map(owner_renter_mapper)
    merged["StageName"] = merged["StageName"].map(stage_mapper)
    merged["Cancelation_Reason_s__c"] = ""
    merged["Do_Not_Contact__c"] = False
    merged.loc[merged["Lead Status"] != "Visit Scheduled", "StageName"] = (
        "Not Yet Scheduled"
    )
    merged.loc[merged["Lead Status"] == "Not Eligible", "StageName"] = "Canceled"
    merged.loc[merged["Lead Status"] == "Not Eligible", "Cancelation_Reason_s__c"] = (
        "5+ units"
    )
    merged.loc[~merged["Weatherization_Date_Time__c_y"].isna(), "StageName"] = (
        "Signed Contracts"
    )
    merged.loc[
        ~merged["Weatherization_Date_Time__c_y"].isna(), "Weatherization_Status__c"
    ] = "Completed"
    merged.loc[merged["StageName"].isna(), "StageName"] = "Canceled"
    merged.loc[
        merged["HEA Rescheduling Status"] == "Do Not Contact", "Do_Not_Contact__c"
    ] = True
    merged.loc[
        (merged["StageName"] == "Canceled") & (merged["Lead Status"] != "Not Eligible"),
        "Cancelation_Reason_s__c",
    ] = "No Reason"
    merged.loc[
        (merged["StageName"] == "Canceled")
        & (merged["Cancelation_Reason_s__c"].isna()),
        "Cancelation_Reason_s__c",
    ] = "No Reason"
    merged["CloseDate"] = merged["CloseDate"].fillna(merged["HEA_Date_And_Time__c"])
    merged["CloseDate"] = merged["CloseDate"].fillna(
        merged["Weatherization_Date_Time__c"]
    )

    merged["Name"] = merged["FirstName"] + " " + merged["LastName"]

    merged["Street__c"] = (
        merged["Street__c"] + ", " + merged["City__c"] + ", MA, " + merged["Zipcode__c"]
    )

    for date_column in [
        "CloseDate",
        "HEA_Date_And_Time__c",
        "Weatherization_Date_Time__c",
        "Original_HEA_Date_Time__c",
    ]:
        merged[date_column] = to_sf_datetime(merged[date_column], REVISE_DATE_FORMAT)

    merged["PersonEmail"] = merged["PersonEmail"].apply(toSalesforceEmail)
    merged["Phone"] = merged["Phone"].apply(toSalesforcePhone)
    merged["Prefered_Lan__c"] = merged["Prefered_Lan__c"].map(language_mapper)
    merged = merged.drop_duplicates(["ID_from_HPC__c"])
    return merged
