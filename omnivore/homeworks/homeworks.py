import re
import pandas as pd
import numpy as np
import logging
import time

pd.set_option("display.max_columns", 1000)
pd.options.mode.chained_assignment = None  # type:ignore
from omnivore.utils.aux import toSalesforceEmail, toSalesforcePhone

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

# Health And Safety
hsMapper = {
    "Knob and Tube": "Knob & Tube (Major)",
    "Asbestos": "Asbestos",
    "Vermiculite": "Vermiculite",
    "Mold": "Mold/Moisture",
}

def combine_hs(row: pd.Series) -> str | float:
    """
    Unfortunately, homeworks uses columns with a boolean value for
    each Health and Safety Barrier category. This function combines
    them in one column.
    """
    try:
        values = [value for key, value in hsMapper.items() if row[key] == "1"]
        return ";".join(values) or np.nan
    except Exception as e:
        logger.error("An error occurred:", str(e))


def rename_and_merge(homeworks_old_input, homeworks_new_input) -> pd.DataFrame:
    """
    Homeworks sends 2 files. Old input only contains completed HEAs whereas
    new input contains canceled and scheduled ones. Sometimes they overlap.
    This functions rename the columns and combines them in case there's an
    overlap.
    """
    try:
        homeworks_new_input = homeworks_new_input.rename(
            columns={
                "Account: Primary Contact: First Name": "FirstName",
                "Account: Primary Contact: Last Name": "LastName",
                "Phone Number": "Phone",
                "Customer Email": "PersonEmail",
                "Account: Deal: HEA Visit Result Detail": "StageName",
                "Operations: Account: Landlord, Owner, Tenant": "Owner_Renter__c",
                "Operations: Completion/Walk Date": "Weatherization_Date_Time__c",
                "Operations: Operations ID & Payzer Memo": "ID_from_HPC__c",
                "Operations: Job Status": "Weatherization_Status__c",
                "Operations: Billing Town": "City__c",
                "Account: Account Name": "Street__c",
                "Operations: Last Scheduled HEA Date": "HEA_Date_And_Time__c",
                "Reason for Canceled": "Cancelation_Reason_s__c",
            }
        )
        homeworks_new_input["CloseDate"] = homeworks_new_input["HEA_Date_And_Time__c"]
    except Exception as e:
        logger.error("An error occurred:", str(e))

    try:
        homeworks_old_input = homeworks_old_input.rename(
            columns={
                "Customer First Name": "FirstName",
                "Customer Last Name": "LastName",
                "Day Phone": "Phone",
                "Email": "PersonEmail",
                "HEA Visit Result Detail": "StageName",
                "Landlord, Owner, Tenant": "Owner_Renter__c",
                "Operations: Scheduled Insulation Start Date (DT)": "Weatherization_Date_Time__c",
                "Operations: Operations ID & Payzer Memo": "ID_from_HPC__c",
                "Preferred Language": "Prefered_Lan__c",
                "Time Stamp HEA Performed": "HEA_Date_And_Time__c",
                "Created Date": "CloseDate",
                "Wx Job Status": "Weatherization_Status__c",
                "Billing City": "City__c",
                "Account Name": "Street__c",
            }
        )
    except Exception as e:
        logger.error("An error occurred:", str(e))

    try:
        return pd.merge(
            left=homeworks_new_input,
            right=homeworks_old_input,
            how="outer",
            on="ID_from_HPC__c",
        )
    except Exception as e:
        logger.error("An error occurred:", str(e))


def homeworks(homeworks_output):
    stageMapper = {
        "Approval - Customer Unresponsive": "Canceled",
        "Approval / Task Not Completed in Time": "Canceled",
        "Cancel At Door": "Canceled",
        "Closed Lost": "Signed Contracts",
        "Closed Won": "Signed Contracts",
        "Closed Won - Pending QC": "Signed Contracts",
        "Covid Related": "Canceled",
        "Customer Cancel Request": "Canceled",
        "Customer Cancellation Request": "Canceled",
        "Customer No-Show": "Canceled",
        "Customer Reschedule Request": "Canceled",
        "HEA Not Approved": "Canceled",
        "HES Cancel": "Canceled",
        "High Probability": "Recommended - Unsigned",
        "High Probability - Pending QC": "Recommended - Unsigned",
        "HWE Cancel": "Canceled",
        "ICW Fixed- Pending Review": "Recommended - Unsigned",
        "Incorrectly Closed Won": "No Opportunity",
        "Low Probability": "Recommended - Unsigned",
        "Not approved - Mileage": "Canceled",
        "Not in EM Home": "No Opportunity",
        "Office Cancel": "Canceled",
        "Overbooking Cancel": "Canceled",
        "Overbooking Reschedule": "Canceled",
        "PreWeatherization Barrier": "Health & Safety Barrier",
        "PreWeatherization Barrier - Pending QC": "Health & Safety Barrier",
        "Qualified Out": "No Opportunity",
        "Scheduling Error": "Canceled",
        "Unqualified - 5+ Units": "Canceled",
        "Unqualified - Had Previous Assessment": "Canceled",
        "Unqualified - Low Income": "Canceled",
        "Unqualified - Other": "Canceled",
        "Unqualified (5+ Units)": "Canceled",
        "Visit Not Confirmed": "Canceled",
        "": "Scheduled",
        np.nan: "Scheduled",
    }

    # Cancel Reason
    cancelMapper = {
        "Approval - Customer Unresponsive": "Unresponsive",
        "Scheduling Error": "By Office",
        "Customer Reschedule Request": "Reschedule Request",
        "Approval / Task Not Completed in Time": "By Office",
        "Cancel At Door": "By Customer",
        "Overbooking Cancel": "By Office",
        "Customer No-Show": "Customer No Show",
        "Office Cancel": "By Office",
        "Customer Cancellation Request": "Reschedule Request",
        "HEA not Approved": "By Office",
        "Impromptu": "By Customer",
        "HWE Cancel": "By Office",
        "Covid Related": "Reschedule Request",
        "Overbooking Reschedule": "By Office",
        "Unqualified - Low Income": "Low Income",
        "Other": "No reason",
        "Unqualified - Had Previous Assessment": "Had HEA within 2 years",
        "HES cancel": "By Office",
        "Unqualified - Other": "No reason",
        "Unqualified (5+ Units)": "5+ units",
        "Unqualified - 5+ Units": "5+ units",
        "Visit Not Confirmed": "By Office",
    }

    # // Weatherization Status
    wxMapper = {
        "1st Scheduling Attempt": "Not Yet Scheduled",
        "2nd Scheduling Attempt": "Not Yet Scheduled",
        "3rd Scheduling Attempt": "Not Yet Scheduled",
        "4th Scheduling Attempt": "Not Yet Scheduled",
        "5th Scheduling Attempt": "Not Yet Scheduled",
        "6th Attempt - Next Attempt is Final": "Not Yet Scheduled",
        "Cancelled [Declined Work When We Attempted To Schedule]": "Not Yet Scheduled",
        "Confirm Schedule Date 1st Attempt Completed": "Scheduled",
        "Confirm Schedule Date 2nd Attempt Completed": "Scheduled",
        "Confirm Schedule Date 3rd Attempt Completed": "Scheduled",
        "Dead [Couldn't Get In Contact With The Customer]": "Not Yet Scheduled",
        "Installed and Invoiced": "Completed",
        "Installed- NOT Invoiced": "Completed",
        "Needs to be rescheduled - Permit Denied": "Not Yet Scheduled",
        "Needs to be rescheduled- HWE Canceled": "Not Yet Scheduled",
        "Needs to be Rescheduled- Unable to Confirm Scheduled Date": "Not Yet Scheduled",
        "Not Ready for Scheduling": "Not Yet Scheduled",
        "Ready for Scheduling": "Not Yet Scheduled",
        "Schedule Date Confirmed": "Scheduled",
        "Scheduled IH- Pending Confirmation": "Scheduled",
        "Scheduled with Sub": "Scheduled",
        "Walk- Cannot be Recovered": "Not Yet Scheduled",
        "Walk- In Recovery Process": "Not Yet Scheduled",
        "Wx Scheduling Case": "Not Yet Scheduled",
        "Installed - Docs Uploaded": "Completed",
        "Needs to be rescheduled - customer request/customer no show": "Not Yet Scheduled",
        "2nd Scheduling Attempt": "Not Yet Scheduled",
        "3rd Scheduling Attempt": "Not Yet Scheduled",
        "4th Scheduling Attempt": "Not Yet Scheduled",
        "Sent to Installer": "Scheduled",
        "Needs to be rescheduled - Robocall": "Not Yet Scheduled",
        "Partial Complete": "Completed",
    }

    # // Owner/Renter
    orMapper = {
        "Landlord": "Owner",
        "Owner-Occupied": "Owner",
        "own": "Owner",
        "Tenant": "Renter",
    }

    #     // Combine both data
    try:
        homeworks_output["FirstName"] = homeworks_output["FirstName_x"].combine_first(
            homeworks_output["FirstName_y"]
        )
        homeworks_output["LastName"] = homeworks_output["LastName_x"].combine_first(
            homeworks_output["LastName_y"]
        )
        homeworks_output["StageName"] = homeworks_output["StageName_x"].combine_first(
            homeworks_output["StageName_y"]
        )
        homeworks_output["CloseDate"] = homeworks_output["CloseDate_y"].combine_first(
            homeworks_output["CloseDate_x"]
        )
        homeworks_output["Phone"] = homeworks_output["Phone_x"].combine_first(
            homeworks_output["Phone_y"]
        )
        homeworks_output["PersonEmail"] = homeworks_output["PersonEmail_x"].combine_first(
            homeworks_output["PersonEmail_y"]
        )
        homeworks_output["HEA Visit Result"] = homeworks_output[
            "HEA Visit Result_x"
        ].combine_first(homeworks_output["HEA Visit Result_y"])
        homeworks_output["City__c"] = homeworks_output["City__c_x"].combine_first(
            homeworks_output["City__c_y"]
        )
        homeworks_output["Weatherization_Status__c"] = homeworks_output[
            "Weatherization_Status__c_x"
        ].combine_first(homeworks_output["Weatherization_Status__c_y"])
        homeworks_output["Weatherization_Date_Time__c"] = homeworks_output[
            "Weatherization_Date_Time__c_x"
        ].combine_first(homeworks_output["Weatherization_Date_Time__c_y"])
        homeworks_output["Owner_Renter__c"] = homeworks_output[
            "Owner_Renter__c_x"
        ].combine_first(homeworks_output["Owner_Renter__c_y"])
    except Exception as e:
        logger.error("An error occurred:", str(e))

    try:
        homeworks_output["StageName"] = homeworks_output["StageName"].map(stageMapper)
        homeworks_output["Cancelation_Reason_s__c"] = homeworks_output[
            "Cancelation_Reason_s__c"
        ].map(cancelMapper)

        homeworks_output["Weatherization_Status__c"] = homeworks_output[
            "Weatherization_Status__c"
        ].map(wxMapper)

        homeworks_output["Owner_Renter__c"] = homeworks_output["Owner_Renter__c"].map(
            orMapper
        )
    except Exception as e:
        logger.error("An error occurred:", str(e))

    try:
        homeworks_output["CloseDate"] = pd.to_datetime(homeworks_output["CloseDate"])
        homeworks_output["CloseDate"] = homeworks_output["CloseDate"].dt.strftime(
            "%Y-%m-%d"
        )
        homeworks_output["CloseDate"] = homeworks_output["CloseDate"].astype(str)
        homeworks_output["CloseDate"] = (
            homeworks_output["CloseDate"] + "T00:00:00.000-07:00"
        )
        homeworks_output.loc[
            homeworks_output["CloseDate"] == "nanT00:00:00.000-07:00",
            "CloseDate",
        ] = ""
    except Exception as e:
        logger.error("An error occurred:", str(e))

    try:
        homeworks_output["HEA_Date_And_Time__c"] = homeworks_output[
            "HEA_Date_And_Time__c_y"
        ].combine_first(homeworks_output["HEA_Date_And_Time__c_x"])
        homeworks_output["HEA_Date_And_Time__c"] = pd.to_datetime(
            homeworks_output["HEA_Date_And_Time__c"]
        )
        homeworks_output["HEA_Date_And_Time__c"] = homeworks_output[
            "HEA_Date_And_Time__c"
        ].dt.strftime("%Y-%m-%d")
        homeworks_output["HEA_Date_And_Time__c"] = homeworks_output[
            "HEA_Date_And_Time__c"
        ].astype(str)
        homeworks_output["HEA_Date_And_Time__c"] = (
            homeworks_output["HEA_Date_And_Time__c"] + "T00:00:00.000-07:00"
        )
        homeworks_output.loc[
            homeworks_output["HEA_Date_And_Time__c"] == "nanT00:00:00.000-07:00",
            "HEA_Date_And_Time__c",
        ] = ""
    except Exception as e:
        logger.error("An error occurred:", str(e))

    try:
        homeworks_output["Weatherization_Date_Time__c"] = pd.to_datetime(
            homeworks_output["Weatherization_Date_Time__c"], errors="coerce"
        )
        homeworks_output["Weatherization_Date_Time__c"] = homeworks_output[
            "Weatherization_Date_Time__c"
        ].dt.strftime("%Y-%m-%d")
        homeworks_output["Weatherization_Date_Time__c"] = homeworks_output[
            "Weatherization_Date_Time__c"
        ].astype(str)
        homeworks_output["Weatherization_Date_Time__c"] = (
            homeworks_output["Weatherization_Date_Time__c"] + "T00:00:00.000-07:00"
        )
        homeworks_output.loc[
            homeworks_output["Weatherization_Date_Time__c"] == "nanT00:00:00.000-07:00",
            "Weatherization_Date_Time__c",
        ] = ""
    except Exception as e:
        logger.error("An error occurred:", str(e))

    try:
        homeworks_output["Street__c"] = homeworks_output["Street__c_x"].combine_first(
            homeworks_output["Street__c_y"]
        )
        homeworks_output["Street__c"] = homeworks_output["Street__c"].str.extract(
            r"(\d+ [a-zA-Z]\w{2,} \w{1,})"
        )
    except Exception as e:
        logger.error("An error occurred:", str(e))

    homeworks_output["HPC__c"] = "0013i00000AtGGeAAN"

    try:
        for i in homeworks_output["Phone"].index:
            homeworks_output["Phone"][i] = re.sub(
                r"[^0-9]", "", str(homeworks_output["Phone"][i])
            )
            if len(homeworks_output["Phone"][i]) < 10:
                homeworks_output["Phone"][i] = ""

        for i in homeworks_output["PersonEmail"].index:
            homeworks_output["PersonEmail"][i] = homeworks_output["PersonEmail"][i].lower()
            regex = r"\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b"
            if re.fullmatch(regex, homeworks_output["PersonEmail"][i]):  # type:ignore
                homeworks_output["PersonEmail"]
        
        homeworks_output["PersonEmail"] = homeworks_output["PersonEmail"].replace(
            "na@hwe.com", np.nan
        )
    except Exception as e:
        logger.error("An error occurred:", str(e))

    # Combine health and safety
    try:
        homeworks_output["Health_Safety_Barrier__c"] = homeworks_output.apply(
            combine_hs, axis=1
        )
    except Exception as e:
        logger.error("An error occurred:", str(e))

    homeworks_output = homeworks_output.loc[
        :,
        [
            col
            for col in [
                "PersonEmail",
                "FirstName",
                "LastName",
                "HEA_Date_And_Time__c",
                "CloseDate",
                "StageName",
                "Owner_Renter__c",
                "Street__c",
                "City__c",
                "ID_from_HPC__c",
                "Weatherization_Status__c",
                "Weatherization_Date_Time__c",
                "Cancelation_Reason_s__c",
                "Phone",
                "Health_Safety_Barrier__c",
                "tempId",
            ]
            if col in homeworks_output.columns
        ],
    ]

    return homeworks_output