import pandas as pd

pd.set_option('display.max_columns', None)

homeworks_input = pd.read_csv ('/content/CFP Communities Report - Homeworks Input.csv')
homeworks_output = homeworks_input
output = pd.read_csv ('/content/CFP Communities Report - Output.csv')

# // Neeeco words into Salesforce Stage
stageMapper = {
  'Closed Won': 'Signed Contracts',
  'Closed Won - Pending QC': 'Signed Contracts',
  'High Probability': 'Recommended - Unsigned',
  'High Probability - Pending QC': 'Recommended - Unsigned',
  'ICW Fixed- Pending Review': 'Signed Contracts',
  'Incorrectly Closed Won': 'Recommended - Unsigned',
  'Low Probability': 'Recommended - Unsigned',
  'Not in EM Home': 'Signed Contracts',
  'PreWeatherization Barrier': 'Health & Safety Barrier',
  'PreWeatherization Barrier - Pending QC': 'Health & Safety Barrier',
  'Qualified Out': 'No Opportunity',
  'Closed Lost': 'Signed Contracts'
}

# // Weatherization Status
wxMapper = {
  '1st Scheduling Attempt': 'Not Yet Scheduled',
  '2nd Scheduling Attempt': 'Not Yet Scheduled',
  '3rd Scheduling Attempt': 'Not Yet Scheduled',
  '5th Scheduling Attempt': 'Not Yet Scheduled',
  "6th Attempt - Next Attempt is Final": 'Not Yet Scheduled',
  "Cancelled [Declined Work When We Attempted To Schedule]": "Canceled",
  "Confirm Schedule Date 2nd Attempt Completed": "Scheduled",
  "Confirm Schedule Date 3rd Attempt Completed": "Scheduled",
  "Dead [Couldn't Get In Contact With The Customer]": "Canceled",
  "Installed and Invoiced": "Completed",
  "Installed- NOT Invoiced": "Completed",
  "Needs to be rescheduled - Permit Denied": "Not Yet Scheduled",
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
  "Needs to be rescheduled - customer request/customer no show": 'Not Yet Scheduled',
  '4th Scheduling Attempt': 'Not Yet Scheduled',
  "Needs to be rescheduled - Robocall": 'Not Yet Scheduled',
  "Partial Complete": "Completed"
}

# // Owner/Renter
orMapper = {
  'Landlord': 'Owner',
  'Owner-Occupied': 'Owner',
  'own': 'Owner',
  'Tenant': 'Renter'
}

homeworks_output=homeworks_output.rename(columns={"Customer First Name": "FirstName",
                                                  "Customer Last Name":"LastName",
                                                  "Day Phone":"Phone",
                                                  "Email":"PersonEmail",
                                                  "HEA Visit Result Detail":"StageName",
                                                  "Landlord, Owner, Tenant":"Owner_Renter__c",
                                                  "Operations: Scheduled Insulation Start Date (DT)":
                                                  "Weatherization_Date_Time__c",
                                                  "Operations: Unique ID":"ID_from_HPC__c",
                                                  "Preferred Language":"Prefered_Lan__c",
                                                  "Time Stamp HEA Performed":"HEA_Date_And_Time__c",
                                                  "Time Stamp HEA Performed":"CloseDate",
                                                  "Wx Job Status":"Weatherization_Status__c",
                                                  "Billing City": "City__c",
                                                  "Account Name": "Name"})

homeworks_output['StageName'] = homeworks_output[
    'StageName'].map(stageMapper)

homeworks_output['Weatherization_Status__c'] = homeworks_output[
    'Weatherization_Status__c'].map(wxMapper)

homeworks_output['Owner_Renter__c'] = homeworks_output[
    'Owner_Renter__c'].map(orMapper)

homeworks_output['CloseDate'] = pd.to_datetime(homeworks_output['CloseDate'])
homeworks_output['CloseDate'] = homeworks_output['CloseDate'].dt.strftime('%Y-%m-%d')
homeworks_output['CloseDate'] = homeworks_output['CloseDate']+'T00:00:00.000-07:00'
homeworks_output['HEA_Date_And_Time__c']=homeworks_output['CloseDate']

homeworks_output['Weatherization_Date_Time__c'] = pd.to_datetime(homeworks_output['Weatherization_Date_Time__c'], errors='coerce')
homeworks_output['Weatherization_Date_Time__c'] = homeworks_output['Weatherization_Date_Time__c'].dt.strftime('%Y-%m-%d')+'T00:00:00.000-07:00'

homeworks_output["Street__c"] = homeworks_output[
    "Name"].str.extract(r'(\d+ [a-zA-Z]\w{2,} \w{1,})')
homeworks_output["City__c"] = homeworks_output[
    "Name"].str.extract(r'([a-zA-Z]\w{2,}$)')
homeworks_output["Name"] = homeworks_output[
    "Name"].str.extract(r'(^[a-zA-Z]\w{2,} \w{1,})')

homeworks_output['HPC__c'] = '0013i00000AtGGeAAN'


homeworks_output=homeworks_output.loc[:,['FirstName', 'LastName', 'Street__c', 'City__c', 'Name',
       'HEA_Date_And_Time__c', 'Weatherization_Date_Time__c', 'CloseDate',
       'StageName', 'Weatherization_Status__c', 'ID_from_HPC__c',
       'HPC__c', 'Prefered_Lan__c',
       'Owner_Renter__c', 'Phone', 'PersonEmail']]

# print(homeworks_output[homeworks_output["ID_from_HPC__c"].isin(['a0o4X00000Kp5pHQAR','a0o4X00000L7XIJQA3','a0o4X00000JxemSQAR'])])