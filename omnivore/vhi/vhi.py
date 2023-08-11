from omnivore.utils.aux import toSalesforceEmail, toSalesforcePhone
from pandas import DataFrame, to_datetime, Series
from datetime import datetime
from numpy import nan
import logging
import time

logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s - %(levelname)s - %(message)s",
    filename="logger.log",
    filemode="w",
)

# Create a logger object
logger = logging.getLogger()

# Remove existing handlers to avoid duplication
for handler in logger.handlers[:]:
    logger.removeHandler(handler)

# Create a file handler and set its level
file_handler = logging.FileHandler("logger.log")
file_handler.setLevel(logging.DEBUG)

# Create a formatter and set it for the file handler
formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
file_handler.setFormatter(formatter)

# Add the file handler to the logger
logger.addHandler(file_handler)

stageMapper = {
    "Recommended - Unsigned": "Recommended - Unsigned",
    "Permit Submitted": "Recommended - Unsigned",
    "Proposal/Price Quote": "Recommended - Unsigned",
    "Permit Approved": "Recommended - Unsigned",
    "Wx Completed": "Signed Contracts",
    "Wx Scheduled": "Signed Contracts",
    "Rescheduling Process": "Canceled",
    "Canceled": "Canceled",
    "Health & Safety Barrier": "Health & Safety Barrier",
    "No Opportunity": "No Opportunity",
    "Scheduled": "Scheduled",
}


def clean_contact_info(row) -> Series:
    try:
        row["PersonEmail"] = toSalesforceEmail(row["PersonEmail"]) or nan
        row["Phone"] = toSalesforcePhone(row["Phone"]) or nan
        return row
    except Exception as e:
        logger.error("An error occurred: %s", str(e))


def vhi(data: DataFrame) -> DataFrame:
    # Removing unprocessable rows
    try:
        data = data[~data["VHI Unique Number"].isna()]
        data = data[~data["Contact: Email"].isna() | ~data["Contact: Phone"].isna()]
    except Exception as e:
        logger.error("An error occurred: %s", str(e))

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
            }
        )
    except Exception as e:
        logger.error("An error occurred: %s", str(e))

    # Combine street and city
    try:
        data["Street__c"] = data["Address"] + " " + data["Billing City"]
    except Exception as e:
        logger.error("An error occurred: %s", str(e))

    # Translate stage into stagename and wx status
    try:
        data["Weatherization_Status__c"] = nan
        data.loc[
            (data["StageName"] == "Wx Scheduled"), "Weatherization_Status__c"
        ] = "Scheduled"
        data.loc[
            (data["StageName"] == "Wx Completed"), "Weatherization_Status__c"
        ] = "Completed"
        data["StageName"] = data["StageName"].map(stageMapper)
        data.loc[(data["StageName"].isna()), "StageName"] = "Canceled"
        data["Cancelation_Reason_s__c"] = nan
        data.loc[
            (data["StageName"] == "Canceled"), "Cancelation_Reason_s__c"
        ] = "No Reason"
        data.loc[
            (data["Lead Vendor"] == "ABCD"), "Cancelation_Reason_s__c"
        ] = "Low Income"
    except Exception as e:
        logger.error("An error occurred: %s", str(e))

    # Date and Time
    try:
        data["CloseDate"] = to_datetime(
            data["HEA_Date_And_Time__c"], errors="coerce"
        ).dt.strftime("%Y-%m-%d" + "T" + "%H:%M:%S" + ".000-07:00")
        data.loc[(data["CloseDate"].isna()), "CloseDate"] = datetime.now().strftime(
            "%Y-%m-%d" + "T" + "%H:%M:%S" + ".000-07:00"
        )
        data["HEA_Date_And_Time__c"] = to_datetime(
            data["HEA_Date_And_Time__c"], errors="coerce"
        ).dt.strftime("%Y-%m-%d" + "T" + "%H:%M:%S" + ".000-07:00")
        data["Weatherization_Date_Time__c"] = to_datetime(
            data["Weatherization_Date_Time__c"], errors="coerce"
        ).dt.strftime("%Y-%m-%d" + "T" + "%H:%M:%S" + ".000-07:00")
    except Exception as e:
        logger.error("An error occurred: %s", str(e))

    # first name and last name
    try:
        data["FirstName"] = data["Opportunity Name"].str.extract(
            r"^(.*?)\s(?:\S+\s)*\S+$"
        )
        data["LastName"] = data["Opportunity Name"].str.extract(r"\s(.*)$")
    except Exception as e:
        logger.error("An error occurred: %s", str(e))

    # Clean up contact info
    try:
        data = data.apply(clean_contact_info, axis=1)
    except Exception as e:
        logger.error("An error occurred: %s", str(e))

    return data

    # # Calculate rows added in the file
    # new = len(data) - len(neeeco_input)
    # logging.debug(f"Number of new rows added in VHI : {new}")

    # # Calculate rows updated in the file
    # update = len(data) - new
    # logging.debug(f"Number of rows updated in VHI : {update}")

    # end_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
    # # Log an info message with the end time
    # logger.debug(f"VHI execution ended at: {end_time}")
