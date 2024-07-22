from omnivore.utils.aux import (
    toSalesforceEmail,
    toSalesforcePhone,
    save_output_df,
    to_sf_datetime,
    DATETIME_SALESFORCE,
    extract_zipcode,
)
from pandas import DataFrame, Series
from datetime import datetime
from numpy import nan


# Create a logger object
from omnivore.utils.logging import getLogger

logger = getLogger(__name__)

stageMapper = {
    "Recommended - Unsigned": "Recommended - Unsigned",
    "Permit Submitted": "Recommended - Unsigned",
    "Proposal/Price Quote": "Recommended - Unsigned",
    "Permit Approved": "Recommended - Unsigned",
    "Wx Completed": "Signed Contracts",
    "Wx Scheduled": "Signed Contracts",
    "Wx Cancelled": "Signed Contracts",
    "Complete": "No Opportunity",
    "Rescheduling Process": "Canceled",
    "Canceled": "Canceled",
    "Health & Safety Barrier": "Health & Safety Barrier",
    "No Opportunity": "No Opportunity",
    "Scheduled": "Scheduled",
    "Sent to Rise for Approval": "Not Yet Scheduled",
    "Qualified": "Not Yet Scheduled",
    "Not Yet Scheduled": "Not Yet Scheduled",
}

wxMapper = {
    "Wx Completed": "Completed",
    "Wx Scheduled": "Scheduled",
    "Wx Cancelled": "Canceled",
}


def clean_contact_info(row) -> Series:
    try:
        row["PersonEmail"] = toSalesforceEmail(row["PersonEmail"]) or nan
        row["Phone"] = toSalesforcePhone(row["Phone"]) or nan
        return row
    except Exception as e:
        logger.error("An error occurred: %s", str(e), exc_info=True)


def vhi(data: DataFrame) -> DataFrame:
    # Removing unprocessable rows
    try:
        data = data[~data["VHI Unique Number"].isna()]
        # data = data[~data["Contact: Email"].isna() | ~data["Contact: Phone"].isna()]
    except Exception as e:
        logger.error("An error occurred: %s", str(e), exc_info=True)
    # Rename columns into Salesforce Field
    try:
        data = data.rename(
            columns={
                "VHI Unique Number": "ID_from_HPC__c",
                "Contact: Phone": "Phone",
                "Contact: Email": "PersonEmail",
                "Audit Date & Time": "HEA_Date_And_Time__c",
                "Stage": "StageName",
                "Health & Safety Issues": "Health_Safety_Barrier__c",
                "Date of work": "Weatherization_Date_Time__c",
                "Amount": "Final_Contract_Amount__c",
                "Preferred Language": "Prefered_Lan__c",
                "Mailing Zip/Postal Code": "Zipcode__c",
            }
        )
    except Exception as e:
        logger.error("An error occurred: %s", str(e), exc_info=True)

    # Combine street and city
    try:
        data["Street__c"] = data["Address"] + " " + data["Billing City"]
    except Exception as e:
        logger.error("An error occurred: %s", str(e), exc_info=True)

    # Translate stage into stagename and wx status
    try:
        data["Weatherization_Status__c"] = data["StageName"].map(wxMapper)
        data["StageName"] = data["StageName"].map(stageMapper)
        data.loc[(data["StageName"].isna()), "StageName"] = "Canceled"
        data["Cancelation_Reason_s__c"] = ""
        data.loc[(data["StageName"] == "Canceled"), "Cancelation_Reason_s__c"] = (
            "No Reason"
        )
        data.loc[(data["Lead Vendor"] == "ABCD"), "Cancelation_Reason_s__c"] = (
            "Low Income"
        )
        data.loc[
            (data["Health_Safety_Barrier__c"].fillna("").str.contains("Fixed")),
            "Health_Safety_Barrier__c",
        ] = nan
    except Exception as e:
        logger.error("An error occurred: %s", str(e), exc_info=True)

    # Date and Time
    try:
        data["CloseDate"] = to_sf_datetime(data["Created Date"], "%m/%d/%Y")
        data.loc[(data["CloseDate"].isna()), "CloseDate"] = datetime.now().strftime(
            DATETIME_SALESFORCE
        )
        data["HEA_Date_And_Time__c"] = to_sf_datetime(
            data["HEA_Date_And_Time__c"], "%m/%d/%Y, %I:%M %p"
        )
        data["Weatherization_Date_Time__c"] = to_sf_datetime(
            data["Weatherization_Date_Time__c"], "%m/%d/%Y, %I:%M %p"
        )
    except Exception as e:
        logger.error("An error occurred: %s", str(e), exc_info=True)

    # first name and last name
    try:
        data["FirstName"] = data["Opportunity Name"].str.extract(
            r"^(.*?)\s(?:\S+\s)*\S+$"
        )
        data["LastName"] = data["Opportunity Name"].str.extract(r"\s(.*)$")
    except Exception as e:
        logger.error("An error occurred: %s", str(e), exc_info=True)

    # Clean up contact info
    try:
        data = data.apply(clean_contact_info, axis=1)
    except Exception as e:
        logger.error("An error occurred: %s", str(e), exc_info=True)

    no_address_removed = data[~data["Address"].isna()]
    no_address_removed["FirstName"] = no_address_removed["FirstName"].fillna("")
    no_address_removed["LastName"] = no_address_removed["LastName"].fillna("Unknown")

    no_address_removed["Zipcode__c"] = extract_zipcode(no_address_removed["Zipcode__c"])

    # Owner Renter
    no_address_removed["Owner_Renter__c"] = ""

    no_address_removed.loc[
        no_address_removed["Ownership Status"].fillna("").str.contains("Owner"),
        "Owner_Renter__c",
    ] = "Owner"
    no_address_removed.loc[
        no_address_removed["Ownership Status"].fillna("").str.contains("Renter"),
        "Owner_Renter__c",
    ] = "Renter"

    # Low Income
    no_address_removed.loc[
        no_address_removed["Mod Income Eligible"].astype(str) == "1",
        "Cancelation_Reason_s__c",
    ] = "Low Income"

    return no_address_removed
    # # Calculate rows added in the file
    # new = len(data) - len(neeeco_input)
    # logging.debug(f"Number of new rows added in VHI : {new}")

    # # Calculate rows updated in the file
    # update = len(data) - new
    # logging.debug(f"Number of rows updated in VHI : {update}")

    # end_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
    # # Log an info message with the end time
    # logger.debug(f"VHI execution ended at: {end_time}")
