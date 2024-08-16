from pandas import Series, DataFrame, merge
import numpy as np

from ..utils.aux import (
    to_sf_datetime,
    toSalesforceEmail,
    toSalesforcePhone,
    combine_xy_columns,
    save_output_df,
    extract_zipcode,
)

# Create a logger object
from omnivore.utils.logging import getLogger

logger = getLogger(__name__)

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
    "CAP Vetting Reschedule": "By Office",
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

# Health And Safety
hsMapper = {
    "Knob and Tube": "Knob & Tube (Major)",
    "Asbestos": "Asbestos",
    "Vermiculite": "Vermiculite",
    "Mold": "Mold/Moisture",
    "Failed CST High CO": "Combustion Safety Failure",
    "Failed CST High CO Detail": "Combustion Safety Failure",
    "Failed CST": "Combustion Safety Failure",
    "Failed CST Spillage": "Combustion Safety Failure",
    "Failed CST Oven/Dryer": "Combustion Safety Failure",
    "Structural Limitations": "Other",
    "Inaccessible Dirt Crawlspace": "Other",
}


def combine_hs(row: Series) -> str | float:
    """
    Unfortunately, homeworks uses columns with a boolean value for
    each Health and Safety Barrier category. This function combines
    them in one column.
    """
    try:
        values = set([value for key, value in hsMapper.items() if row[key] == "1"])
        return ";".join(values) or np.nan
    except Exception as e:
        logger.error("An error occurred: %s", str(e), exc_info=True)


def rename_and_merge(_homeworks_old_input, _homeworks_new_input) -> DataFrame:
    """
    Homeworks sends 2 files. Old input only contains completed HEAs whereas
    new input contains canceled and scheduled ones. Sometimes they overlap.
    This functions rename the columns and combines them in case there's an
    overlap.
    """
    homeworks_new_input = _homeworks_new_input.copy()
    homeworks_old_input = _homeworks_old_input.copy()
    # Bulk upload old data:
    # homeworks_new_input[
    #     "Operations: Operations ID & Payzer Memo"
    # ] = homeworks_new_input["Operations: Operations ID & Payzer Memo"].fillna(
    #     homeworks_new_input["Operations: Deal: Deal ID"]
    # )
    # homeworks_old_input[
    #     "Operations: Operations ID & Payzer Memo"
    # ] = homeworks_old_input["Operations: Operations ID & Payzer Memo"].fillna(
    #     homeworks_old_input["Operations: Unique ID"]
    # )
    try:
        homeworks_new_input["Operations: Last Scheduled HEA Date"] = to_sf_datetime(
            homeworks_new_input["Operations: Last Scheduled HEA Date"],
            format="%m/%d/%Y %I:%M %p",
        )
        homeworks_new_input["Created Date"] = to_sf_datetime(
            homeworks_new_input["Operations: Created Date"],
            format="%m/%d/%Y",
        )

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
                "Created Date": "CloseDate",
                "Operations: Account: Preferred Language": "Prefered_Lan__c",
                "Operations: Account: 5-digit Zip Code": "Zipcode__c",
            }
        )

    except Exception as e:
        logger.error("An error occurred: %s", str(e), exc_info=True)

    try:
        homeworks_old_input["Time Stamp HEA Performed"] = to_sf_datetime(
            homeworks_old_input["Time Stamp HEA Performed"],
            "%m/%d/%Y %I:%M %p",
        )
        homeworks_old_input["Created Date"] = to_sf_datetime(
            homeworks_old_input["Created Date"], "%m/%d/%Y"
        )
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
                "5-digit Zip Code": "Zipcode__c",
            }
        )
    except Exception as e:
        logger.error("An error occurred: %s", str(e), exc_info=True)

    try:
        merged = merge(
            left=homeworks_new_input,
            right=homeworks_old_input,
            how="outer",
            on="ID_from_HPC__c",
        )
        return combine_xy_columns(merged)
    except Exception as e:
        logger.error("An error occurred: %s", str(e), exc_info=True)


def homeworks(homeworks_output):
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
        logger.error("An error occurred: %s", str(e), exc_info=True)

    try:
        # mask = homeworks_output["CloseDate"].isna()
        # homeworks_output.loc[~mask, "CloseDate"] = homeworks_output.loc[
        #     ~mask, "CloseDate"
        # ].dt.strftime(DATETIME_SALESFORCE)
        # homeworks_output["CloseDate"] = homeworks_output["CloseDate"].astype(str)
        homeworks_output["CloseDate"] = homeworks_output["CloseDate"].fillna("")
    except Exception as e:
        logger.error("An error occurred: %s", str(e), exc_info=True)
        logger.error(e)

    try:
        homeworks_output["HEA_Date_And_Time__c"] = homeworks_output[
            "HEA_Date_And_Time__c"
        ].fillna("HEA_Date_And_Time__c")
    except Exception as e:
        logger.error("An error occurred: %s", str(e), exc_info=True)

    try:
        homeworks_output["Weatherization_Date_Time__c"] = to_sf_datetime(
            homeworks_output["Weatherization_Date_Time__c"], "%m/%d/%Y %I:%M %p"
        )
    except Exception as e:
        logger.error("An error occurred: %s", str(e), exc_info=True)

    try:
        homeworks_output["Zipcode__c"] = extract_zipcode(
            homeworks_output["Zipcode__c"]
        ).fillna("")

        homeworks_output["Street__c"] = (
            homeworks_output["Street__c"]
            .fillna("")
            .str.replace(homeworks_output["FirstName"], "", 1, True)
            .fillna("")
            .str.strip()
        )
        homeworks_output["Street__c"] = (
            homeworks_output["Street__c"]
            .fillna("")
            .str.replace(homeworks_output["LastName"], "", 1, True)
            .fillna("")
            .str.strip()
        )
        # homeworks_output["Street__c"] = homeworks_output["Street__c"].str.extract(
        #     r"(\d+ [a-zA-Z]\w{2,} \w{1,})"
        # )
        homeworks_output["Street__c"] = (
            homeworks_output["Street__c"] + ", MA " + homeworks_output["Zipcode__c"]
        )
    except Exception as e:
        logger.error("An error occurred: %s", str(e), exc_info=True)

    try:
        homeworks_output["PersonEmail"] = homeworks_output["PersonEmail"].apply(
            toSalesforceEmail
        )
        homeworks_output["Phone"] = homeworks_output["Phone"].apply(toSalesforcePhone)
        homeworks_output["PersonEmail"] = homeworks_output["PersonEmail"].replace(
            "na@hwe.com", np.nan
        )
    except Exception as e:
        logger.error("An error occurred: %s", str(e), exc_info=True)

    # Combine health and safety
    try:
        homeworks_output["Health_Safety_Barrier__c"] = homeworks_output.apply(
            combine_hs, axis=1
        )
    except Exception as e:
        logger.error("An error occurred: %s", str(e), exc_info=True)

    homeworks_output["Prefered_Lan__c"] = homeworks_output[
        "Prefered_Lan__c"
    ].str.replace("Chinese(Mandarin)", "Mandarin")
    homeworks_output["Prefered_Lan__c"] = homeworks_output[
        "Prefered_Lan__c"
    ].str.replace("Chinese(Cantonese)", "Cantonese")

    return homeworks_output

    # # Calculate rows added in the file
    # new = len(homeworks_output) - len(neeeco_input)
    # logging.debug(f"Number of new rows added in Homeworks : {new}")

    # # Calculate rows updated in the file
    # update = len(homeworks_output) - new
    # logging.debug(f"Number of rows updated in Homeworks : {update}")

    # end_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
    # # Log an info message with the end time
    # logger.debug(f"Homeworks execution ended at: {end_time}")
