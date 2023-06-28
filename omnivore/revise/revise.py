import numpy
import pandas as pd
import re

pd.options.mode.chained_assignment = None

revise_input=pd.read_csv('/Users/ankit/Documents/Omnivore_Refactor/omnivore/revise/Revise Input.csv')
revise_wx_input=pd.read_csv('/Users/ankit/Documents/Omnivore_Refactor/omnivore/revise/Revise Wx Input.csv')

revise_input['Client Number'] = revise_input['Client Number'].astype(object)
revise_wx_input['Client Number'] = revise_wx_input['Client Number'].astype(object)

revise_input = revise_input.rename(columns={
    "City":"City__c",
    "State/Province":"State__c",
    "Street":"Street__c",
    "Zip/Postal Code":"Zip__c",
    "Company / Account":"Account Name",
    "WX Install Date from Event":"WX Install Date"
})

revise_wx_input = revise_wx_input.rename(columns={
    "Billing City":"City__c",
    "Billing State/Province":"State__c",
    "Billing Street":"Street__c",
    "Billing Zip/Postal Code":"Zip__c"
})

# Merge the input dataframes and rename columns
revise_output = revise_input.combine_first(
    revise_wx_input
).rename(columns={
    "Landlord / Owner / Renter": "Owner_Renter__c",
    "Audit Date":"HEA_Date_And_Time__c",
    "Email":"PersonEmail",
    "First Name":"FirstName",
    "Last Name":"LastName",
    "WX Install Date":"Weatherization_Date_Time__c",
    "Created Date":"CloseDate"
})

# Owner / Renter
owner_renter_mapper = {
    "Landlord": 'Owner',
    "Owner":"Owner",
    "Tenant": "Renter",
    "Renter": "Renter"
}

# HEA Date & Time
# Convert to datetime -> Convert to Y/M/D format -> Convert to String -> 
# Concatenate the end -> Remove NaN
revise_output["HEA_Date_And_Time__c"] = pd.to_datetime(
    revise_output["HEA_Date_And_Time__c"], infer_datetime_format=True
)
revise_output["HEA_Date_And_Time__c"] = revise_output["HEA_Date_And_Time__c"].dt.strftime(
    "%Y-%m-%d"
)
revise_output["HEA_Date_And_Time__c"] = revise_output["HEA_Date_And_Time__c"].astype(str)
revise_output["HEA_Date_And_Time__c"] = (
    revise_output["HEA_Date_And_Time__c"] + "T00:00:00.000-07:00"
)
revise_output.loc[
    revise_output["HEA_Date_And_Time__c"] == "nanT00:00:00.000-07:00",
    "HEA_Date_And_Time__c",
] = ""

# Weatherization Date & Time
# Convert to datetime -> Convert to Y/M/D format -> Convert to String -> 
# Concatenate the end -> Remove NaN
revise_output["Weatherization_Date_Time__c"] = pd.to_datetime(
    revise_output["Weatherization_Date_Time__c"], infer_datetime_format=True
)
revise_output["Weatherization_Date_Time__c"] = revise_output["Weatherization_Date_Time__c"].dt.strftime(
    "%Y-%m-%d"
)
revise_output["Weatherization_Date_Time__c"] = revise_output["Weatherization_Date_Time__c"].astype(str)
revise_output["Weatherization_Date_Time__c"] = (
    revise_output["Weatherization_Date_Time__c"] + "T00:00:00.000-07:00"
)
revise_output["Weatherization_Date_Time__c"] = revise_output["Weatherization_Date_Time__c"].replace("nanT00:00:00.000-07:00", "")

# Closed Date
# Convert to datetime -> Convert to Y/M/D format -> Convert to String -> 
# Concatenate the end -> Remove NaN
revise_output["CloseDate"] = pd.to_datetime(
    revise_output["CloseDate"], infer_datetime_format=True
)
revise_output["CloseDate"] = revise_output["CloseDate"].dt.strftime(
    "%Y-%m-%d"
)
revise_output["CloseDate"] = revise_output["CloseDate"].astype(str)
revise_output["CloseDate"] = (
    revise_output["CloseDate"] + "T00:00:00.000-07:00"
)
revise_output.loc[
    revise_output["CloseDate"] == "nanT00:00:00.000-07:00",
    "CloseDate",
] = ""

for i in revise_output["Phone"].index:
    revise_output["Phone"][i] = re.sub(
        r"[^0-9]", "", str(revise_output["Phone"][i])
    )
    if len(revise_output["Phone"][i]) < 10:
        revise_output["Phone"][i] = ""
    if len(revise_output["Phone"][i]) > 10:
        revise_output["Phone"][i] = revise_output["Phone"][i][0:10]

# revise_output["PersonEmail"] = revise_output["Email"].apply(toSalesforceEmail)
revise_output['Owner_Renter__c'] = revise_output['Owner_Renter__c'].map(owner_renter_mapper)

revise_output = revise_output.replace("", numpy.nan)
revise_output = revise_output.loc[
    :,
    [
        col
        for col in [
            
            "FirstName",
            "LastName",
            "HEA_Date_And_Time__c",
            "CloseDate",
            "Account Name",
            "Street__c",
            "City__c",
            "State__c",
            "Zip__c",
            "Weatherization_Date_Time__c"
        ]
        if col in revise_output.columns
    ],
]