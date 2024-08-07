import numpy as np
from pandas import to_datetime, DataFrame, merge
from omnivore.utils.aux import (
    toSalesforceEmail,
    toSalesforcePhone,
    toSalesforceMultiPicklist,
    to_sf_datetime,
    combine_xy_columns,
)
from omnivore.utils.logging import getLogger


NEEECO_DATE_FORMAT = "%m/%d/%Y"


# Create a logger object

logger = getLogger(__name__)


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
    "Damages": "Other",
    "GMB": "Other",
    "Other": "Other",
    "Recessed Lighting": "Other",
}

# // Health And Safety Statuses
hsStageMapper = {
    "H&S Remediated": "Barrier Removed",
    "H&S No Follow Up Needed": "Barrier Removed",
    "H&S Needs Follow Up": "Not Attempted",
}

# Owner / Renter
owner_renter_mapper = {
    "Landlord": "Owner",
    "Tenant": "Renter",
    "Homeowner/Landlord": "Owner",
}


def merge_neeeco(neeeco_input: DataFrame, neeeco_wx_input: DataFrame) -> DataFrame:
    try:
        # Merge the input dataframes and rename columns
        neeeco_output = merge(
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
        neeeco_output["Date Scheduled in Vcita"] = to_sf_datetime(
            neeeco_output["Date Scheduled in Vcita"], NEEECO_DATE_FORMAT
        )

        neeeco_output["Created"] = to_sf_datetime(
            neeeco_output["Created"], NEEECO_DATE_FORMAT
        )
        neeeco_output["CloseDate"] = neeeco_output[
            "Date Scheduled in Vcita"
        ].combine_first(neeeco_output["Created"])

        neeeco_output = combine_xy_columns(neeeco_output)

        return neeeco_output

    except Exception as e:
        logger.error("An error occurred: %s", str(e), exc_info=True)
        return DataFrame()


def neeeco(neeeco_output: DataFrame):

    neeeco_output["Final_Contract_Amount__c"] = neeeco_output[
        "Final Full Job Amount Invoiced"
    ].combine_first(neeeco_output["Customer Final Payment Collected"])

    neeeco_output["Final_Contract_Amount__c"] = neeeco_output[
        "Final_Contract_Amount__c"
    ].fillna("")
    neeeco_output["Contract_Amount__c"] = neeeco_output[
        "$ Total Weatherization Sold_y"
    ].combine_first(neeeco_output["$ Total Weatherization Sold_x"])
    neeeco_output["Contract_Amount__c"] = neeeco_output["Contract_Amount__c"].fillna("")

    try:
        #       // Skip Empty Row
        neeeco_output = neeeco_output[neeeco_output["ID_from_HPC__c"].notnull()]
        neeeco_output["Street__c"] = neeeco_output["Full Address including zip"]
    except Exception as e:
        logger.error("An error occurred: %s", str(e), exc_info=True)

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
        logger.error("An error occurred: %s", str(e), exc_info=True)

    try:
        neeeco_output["HEA_Date_And_Time__c"] = to_sf_datetime(
            neeeco_output["Date of Audit"], NEEECO_DATE_FORMAT
        )
    except Exception as e:
        logger.error("An error occurred: %s", str(e), exc_info=True)

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
        logger.error("An error occurred: %s", str(e), exc_info=True)

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
        neeeco_output["Health_Safety_Barrier__c"] = neeeco_output[
            "Health & Safety Issue"
        ].apply(lambda x: toSalesforceMultiPicklist(x, ", ", hsMapper))
    except Exception as e:
        logger.error("An error occurred: %s", str(e), exc_info=True)

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
        logger.error("An error occurred: %s", str(e), exc_info=True)

    try:
        #       // Wx Jobs Statuses
        neeeco_output["Weatherization_Date_Time__c"] = to_sf_datetime(
            neeeco_output["Insulation Project Installation Date"], NEEECO_DATE_FORMAT
        )

    except Exception as e:
        logger.error("An error occurred: %s", str(e), exc_info=True)

    try:
        neeeco_output["Weatherization_Status__c"] = ""
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
        neeeco_output["Date of Audit"] = to_sf_datetime(
            neeeco_output["Date of Audit"], NEEECO_DATE_FORMAT
        )

        neeeco_output["Date of Audit"].fillna(to_datetime("today"))
        neeeco_output.loc[
            neeeco_output["Date of Audit"] > to_datetime("today"), "StageName"
        ] = "Scheduled"
    except Exception as e:
        logger.error("An error occurred: %s", str(e), exc_info=True)

    try:
        #       // VHEA detection
        neeeco_output["isVHEA__c"] = "FALSE"
        neeeco_output.loc[
            neeeco_output["Related to"].str.contains("VHEA").fillna(False), "isVHEA__c"
        ] = "TRUE"
    except Exception as e:
        logger.error("An error occurred: %s", str(e), exc_info=True)

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
        logger.error("An error occurred: %s", str(e), exc_info=True)

    try:
        neeeco_output["Phone"] = neeeco_output["Phone"].apply(toSalesforcePhone)
    except Exception as e:
        logger.error("An error occurred: %s", str(e), exc_info=True)

    try:
        neeeco_output["PersonEmail"] = neeeco_output["Email"].apply(toSalesforceEmail)
    except Exception as e:
        # Handle other unhandled exceptions
        logger.error("An error occurred: %s", str(e), exc_info=True)

    try:
        neeeco_output["Owner_Renter__c"] = neeeco_output["Owner_Renter__c"].map(
            owner_renter_mapper
        )
        neeeco_output = neeeco_output.replace("", np.nan)
    except Exception as e:
        logger.error("An error occurred: %s", str(e), exc_info=True)

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

    return neeeco_output
