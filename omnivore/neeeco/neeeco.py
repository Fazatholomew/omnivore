import numpy as np
import pandas as pd
import re
from omnivore.utils.aux import toSalesforceEmail
import logging
import time
import sys
from simple_salesforce.exceptions import (
    SalesforceMalformedRequest,
    SalesforceExpiredSession,
    SalesforceResourceNotFound,
    SalesforceGeneralError,
    SalesforceAuthenticationFailed,
)

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

# // Neeeco words into Salesforce Stage
stageMapper = {
    "Customer Declined - No F/U": "Recommended - Unsigned",
    "High Probability": "Recommended - Unsigned",
    "Low Probability": "Recommended - Unsigned",
    "Medium Probability": "Recommended - Unsigned",
    "Sold then Canceled": "Recommended - Unsigned",
    "Deferred Until Later": "No Opportunity",
    "No Opportunity": "No Opportunity",
    "Signed & Sold": "Signed Contracts",
    "OPS H&S Follow Up": "Health & Safety Barrier",
}

# // Cancelation Reasons
neeecoCancelMapper = {
    "Condo - Complex": "5+ units",
    "Unable to Contact/8 attempts": "Unresponsive",
    "Does Not Want Audit": "Not Interested",
    "Low Income": "Low Income",
    "Municipal - Not NG or EV": "Unresponsive",
    "Already Had Audit (2year)": "Had HEA within 2 years",
    "Bad Data": "Bad Data",
    "No Account Info": "Bad Data",
    "Scheduling Conflict": "Reschedule Request",
    "Outside of our territory": "By Office",
    "Under Construction": "By Office",
    "Commercial Property": "5+ units",
    "Do Not Contact": "Not Interested",
    "": "No Reason",
    np.nan: "No Reason",
}

# // Health And Safety Issues
hsMapper = {
    "K&T": "Knob & Tube (Major)",
    "CST": "Combustion Safety Failure",
    "Moisture": "Mold/Moisture",
    "Asbestos": "Asbestos",
}

# // Health And Safety Statuses
hsStageMapper = {
    "H&S Remediated": "Barrier Removed",
    "H&S No Follow Up Needed": "Barrier Removed",
    "H&S Needs Follow Up": "Not Attempted",
}

# Owner / Renter
owner_renter_mapper = {"Landlord": "Owner", "Tenant": "Renter"}


def neeeco(neeeco_input, neeeco_wx_input):
    start_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
    # Log an info message with the start time
    logger.debug(f"Neeeco execution started at: {start_time}")

    try:
        # Merge the input dataframes and rename columns
        neeeco_output = pd.merge(
            left=neeeco_input,
            right=neeeco_wx_input,
            how="left",
            left_on="Related to",
            right_on="HEA - Last, First, Address",
        ).rename(
            columns={
                "ID": "ID_from_HPC__c",
                "Additional Interests": "Legacy_Post_Assessment_Notes__c",
                "Preferred Language": "Prefered_Lan__c",
                "Occupant": "Owner_Renter__c",
            }
        )

    except Exception as e:
        logger.error("An error occurred: %s", str(e))

    #     // Combine both data
    neeeco_output["Final_Contract_Amount__c"] = neeeco_output[
        "Final Full Job Amount Invoiced"
    ].combine_first(neeeco_output["Customer Final Payment Collected"])
    neeeco_output["Insulation Project Status"] = neeeco_output[
        "Insulation Project Status_y"
    ].combine_first(neeeco_output["Insulation Project Status_x"])
    neeeco_output["Contract_Amount__c"] = neeeco_output[
        "$ Total Weatherization Sold_y"
    ].combine_first(neeeco_output["$ Total Weatherization Sold_x"])
    neeeco_output["Insulation Project Installation Date"] = neeeco_output[
        "Insulation Project Installation Date_y"
    ].combine_first(neeeco_output["Insulation Project Installation Date_x"])
    neeeco_output["Health & Safety Status"] = neeeco_output[
        "Health & Safety Status_y"
    ].combine_first(neeeco_output["Health & Safety Status_x"])

    neeeco_output["Final_Contract_Amount__c"] = neeeco_output[
        "Final_Contract_Amount__c"
    ].fillna("")
    neeeco_output["Contract_Amount__c"] = neeeco_output["Contract_Amount__c"].fillna("")

    try:
        #       // Skip Empty Row
        neeeco_output = neeeco_output[neeeco_output["ID_from_HPC__c"].notnull()]
        neeeco_output["Street__c"] = neeeco_output["Full Address including zip"]
    except Exception as e:
        logger.error("An error occurred: %s", str(e))

    try:
        neeeco_output["Name"] = neeeco_output["Name"].str.replace(
            r"[^\w\-&' ]", "", regex=True
        )
        neeeco_output["Name"] = neeeco_output["Name"].str.replace(
            r"( \d\w{1}$| \d)", "", regex=True
        )
        neeeco_output["Name"] = neeeco_output["Name"].str.replace(
            r"( )$", "", regex=True
        )
    except Exception as e:
        logger.error("An error occurred: %s", str(e))

    try:
        neeeco_output["Date Of Audit"] = neeeco_output["Date Of Audit_y"].combine_first(
            neeeco_output["Date Of Audit_x"]
        )
        neeeco_output["Date of Audit"] = neeeco_output["Date of Audit"].replace(
            "", np.nan
        )
        neeeco_output["Date of Audit"] = neeeco_output["Date of Audit"].fillna(
            neeeco_output["Date Of Audit"]
        )
        neeeco_output["Date of Audit"] = pd.to_datetime(neeeco_output["Date of Audit"])
        neeeco_output["Date of Audit"] = neeeco_output["Date of Audit"].dt.strftime(
            "%Y-%m-%d"
        )
        neeeco_output["Date of Audit"] = neeeco_output["Date of Audit"].astype(str)
        neeeco_output["HEA_Date_And_Time__c"] = (
            neeeco_output["Date of Audit"] + "T00:00:00.000-07:00"
        )
        neeeco_output.loc[
            neeeco_output["HEA_Date_And_Time__c"] == "nanT00:00:00.000-07:00",
            "HEA_Date_And_Time__c",
        ] = ""
    except Exception as e:
        logger.error("An error occurred: %s", str(e))

    try:
        neeeco_output["Date Scheduled in Vcita"] = pd.to_datetime(
            neeeco_output["Date Scheduled in Vcita"]
        )
        neeeco_output["Date Scheduled in Vcita"] = neeeco_output[
            "Date Scheduled in Vcita"
        ].dt.strftime("%Y-%m-%d")
        neeeco_output["Date Scheduled in Vcita"] = neeeco_output[
            "Date Scheduled in Vcita"
        ].astype(str)
        neeeco_output["Date Scheduled in Vcita"] = (
            neeeco_output["Date Scheduled in Vcita"] + "T00:00:00.000-07:00"
        )
        neeeco_output["Created"] = pd.to_datetime(
            neeeco_output["Created"], format="%m/%d/%Y"
        )
        neeeco_output["Created"] = neeeco_output["Created"].dt.strftime("%Y-%m-%d")
        neeeco_output["Created"] = neeeco_output["Created"].astype(str)
        neeeco_output["Created"] = neeeco_output["Created"] + "T00:00:00.000-07:00"
        neeeco_output["CloseDate"] = neeeco_output[
            "Date Scheduled in Vcita"
        ].combine_first(neeeco_output["Created"])
        neeeco_output.loc[
            neeeco_output["CloseDate"] == "nanT00:00:00.000-07:00", "CloseDate"
        ] = neeeco_output["Created"]
    except Exception as e:
        logger.error("An error occurred: %s", str(e))

    try:
        neeeco_output["StageName"] = "Scheduled"
        neeeco_output["Cancelation_Reason_s__c"] = ""

        #         // if canceled or disqualified
        neeeco_output.loc[
            neeeco_output["Lead Status"] != "Scheduled (Lead Converted)", "StageName"
        ] = "Canceled"
        neeeco_output.loc[
            neeeco_output["Lead Status"] != "Scheduled (Lead Converted)",
            "Cancelation_Reason_s__c",
        ] = neeeco_output["Lead Disqualified"].map(neeecoCancelMapper)
    except Exception as e:
        logger.error("An error occurred: %s", str(e))

    try:
        #         // Defaulted to No Reason
        neeeco_output.loc[
            (neeeco_output["Lead Status"] != "Scheduled (Lead Converted)")
            & (neeeco_output["Lead Disqualified"] == ""),
            "Cancelation_Reason_s__c",
        ] = "No Reason"

        neeeco_output["Health_Safety_Barrier_Status__c"] = neeeco_output[
            "Health & Safety Status"
        ].map(hsStageMapper)

        neeeco_output["Health & Safety Issue"] = neeeco_output[
            "Health & Safety Issue"
        ].fillna("")
        neeeco_output["Health_Safety_Barrier__c"] = (
            neeeco_output["Health & Safety Issue"]
            .str.replace("K&T", "Knob & Tube (Major)")
            .str.replace("CST", "Combustion Safety Failure")
            .str.replace("Moisture", "Mold/Moisture")
            .str.replace("Asbestos", "Asbestos")
            .str.replace("", "")
            .str.split(",")
            .str.join(";")
            .astype(str)
        )
    except Exception as e:
        logger.error("An error occurred: %s", str(e))

    try:
        # // Audit completed
        neeeco_output.loc[
            (neeeco_output["HEA Status"] == "Completed")
            | (neeeco_output["Insulation Project Status"].notnull()),
            "StageName",
        ] = neeeco_output["Insulation Project Status"].map(stageMapper)
        neeeco_output.loc[
            (neeeco_output["HEA Status"] == "Completed")
            & (neeeco_output["Insulation Project Status"].isnull()),
            "StageName",
        ] = "No Opportunity"

        # # // Although contract signed, still barier
        neeeco_output.loc[
            neeeco_output["Health & Safety Status"] == "H&S Needs Follow Up",
            "StageName",
        ] = "Health & Safety Barrier"
    except Exception as e:
        logger.error("An error occurred: %s", str(e))

    try:
        #       // Wx Jobs Statuses
        neeeco_output["Weatherization_Status__c"] = ""

        neeeco_output["Insulation Project Installation Date"] = pd.to_datetime(
            neeeco_output["Insulation Project Installation Date"]
        )
        neeeco_output["Insulation Project Installation Date"] = neeeco_output[
            "Insulation Project Installation Date"
        ].dt.strftime("%Y-%m-%d")
        neeeco_output["Insulation Project Installation Date"] = neeeco_output[
            "Insulation Project Installation Date"
        ].astype(str)
        neeeco_output["Weatherization_Date_Time__c"] = (
            neeeco_output["Insulation Project Installation Date"]
            + "T00:00:00.000-07:00"
        )
        neeeco_output["Weatherization_Date_Time__c"] = neeeco_output[
            "Weatherization_Date_Time__c"
        ].replace("nanT00:00:00.000-07:00", "")
    except Exception as e:
        logger.error("An error occurred: %s", str(e))

    try:
        # neeeco_output["Weatherization_Status__c"]
        for i in neeeco_output.index:
            if neeeco_output["StageName"][i] == "Signed Contracts":
                if neeeco_output["Final_Contract_Amount__c"][i] != "":
                    neeeco_output["Weatherization_Status__c"][
                        i
                    ] = "Completed"  # Final Completed amount not empty

                elif neeeco_output["Weatherization_Date_Time__c"][i] != "":
                    neeeco_output["Weatherization_Status__c"][i] = "Scheduled"

                elif neeeco_output["Weatherization_Date_Time__c"][i] == "":
                    neeeco_output["Weatherization_Status__c"][
                        i
                    ] = "Not Yet Scheduled"  # If weatherization date is empty

        # // No cancel reason, default it to No Reason
        neeeco_output.loc[
            (neeeco_output["StageName"] == "Canceled")
            & (neeeco_output["Cancelation_Reason_s__c"]).isna(),
            "Cancelation_Reason_s__c",
        ] = "No Reason"

        # // No cancel reason, default it to No Reason
        neeeco_output.loc[
            (neeeco_output["StageName"] == "Canceled")
            & (neeeco_output["Cancelation_Reason_s__c"])
            == "",
            "Cancelation_Reason_s__c",
        ] = "No Reason"

        #         // If date is still in the future, stage is scheduled
        neeeco_output["Date of Audit"] = pd.to_datetime(neeeco_output["Date of Audit"])

        neeeco_output.loc[
            neeeco_output["Date of Audit"] == "", "Date of Audit"
        ] = pd.to_datetime("today")
        neeeco_output.loc[
            neeeco_output["Date of Audit"] > pd.to_datetime("today"), "StageName"
        ] = "Scheduled"
    except Exception as e:
        logger.error("An error occurred: %s", str(e))

    try:
        #       // VHEA detection
        neeeco_output["isVHEA__c"] = "FALSE"
        neeeco_output.loc[
            neeeco_output["Related to"].str.contains("VHEA").fillna(False), "isVHEA__c"
        ] = "TRUE"
    except Exception as e:
        logger.error("An error occurred: %s", str(e))

    neeeco_output["HPC__c"] = "0013i00000AtGAvAAN"
    try:
        neeeco_output["FirstName"] = neeeco_output["Name"].str.extract(
            r"(.*?(?=[\wäöüß]+$))"
        )
        neeeco_output["LastName"] = neeeco_output["Name"].str.extract(r"( \w+)$")
        neeeco_output["FirstName"] = neeeco_output["FirstName"].str.replace(
            r"( )$", "", regex=True
        )
        neeeco_output["LastName"] = neeeco_output["LastName"].str.replace(" ", "")
        neeeco_output["LastName"] = neeeco_output["LastName"].fillna(
            neeeco_output["Name"]
        )
    except Exception as e:
        logger.error("An error occurred: %s", str(e))

    try:
        for i in neeeco_output["Phone"].index:
            neeeco_output["Phone"][i] = re.sub(
                r"[^0-9]", "", str(neeeco_output["Phone"][i])
            )
            if len(neeeco_output["Phone"][i]) < 10:
                neeeco_output["Phone"][i] = ""
            if len(neeeco_output["Phone"][i]) > 10:
                neeeco_output["Phone"][i] = neeeco_output["Phone"][i][0:10]
    except Exception as e:
        logger.error("An error occurred: %s", str(e))

    try:
        neeeco_output["PersonEmail"] = neeeco_output["Email"].apply(toSalesforceEmail)
    except SalesforceMalformedRequest as e:
        # Handle the "Malformed Request" exception
        logger.error("Salesforce Malformed Request:", e)
    except SalesforceExpiredSession as e:
        # Handle the "Expired Session" exception
        logger.error("Salesforce Expired Session:", e)
    except SalesforceResourceNotFound as e:
        # Handle the "Resource Not Found" exception
        logger.error("Salesforce Resource Not Found:", e)
    except SalesforceGeneralError as e:
        # Handle the "General Error" exception
        logger.error("Salesforce General Error:", e)
    except SalesforceAuthenticationFailed as e:
        # Handle the "Authentication Failed" exception
        logger.error("Salesforce Authentication Failed:", e)
    except Exception as e:
        # Handle other unhandled exceptions
        logger.error("Unexpected Error: with Salesforce", e)

    try:
        neeeco_output["Owner_Renter__c"] = neeeco_output["Owner_Renter__c"].map(
            owner_renter_mapper
        )
        neeeco_output = neeeco_output.replace("", np.nan)
    except Exception as e:
        logger.error("An error occurred: %s", str(e))

    neeeco_output = neeeco_output.loc[
        :,
        [
            col
            for col in [
                "Street__c",
                "Name",
                "FirstName",
                "LastName",
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
                "ID_from_HPC__c",
                "Cancelation_Reason_s__c",
                "HPC__c",
                "Phone",
                "Legacy_Post_Assessment_Notes__c",
                "PersonEmail",
                "Owner_Renter__c",
                "Prefered_Lan__c",
                "tempId",
            ]
            if col in neeeco_output.columns
        ],
    ]

    # Calculate rows added in the file
    new = len(neeeco_output) - len(neeeco_input)
    logging.debug(f"Number of new rows added in Neeeco : {new}")

    # Calculate rows updated in the file
    update = len(neeeco_output) - new
    logging.debug(f"Number of rows updated in Neeeco : {update}")

    end_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
    # Log an info message with the end time
    logger.debug(f"Neeeco execution ended at: {end_time}")

    return neeeco_output