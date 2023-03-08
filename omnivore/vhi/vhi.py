from omnivore.utils.aux import toSalesforceEmail, toSalesforcePhone
from pandas import DataFrame, to_datetime, Series
from numpy import nan

stageMapper = {
    'No Opportunity': 'No Opportunity',
    'Recommended - Unsigned': 'Recommended - Unsigned', 
    'Rescheduling Process': 'Canceled',
    'Health & Safety Barrier': 'Health & Safety Barrier',
    'Wx Completed': 'Signed Contracts',
    'Canceled': 'Canceled',
    'Proposal/Price Quote': 'Recommended - Unsigned',
    'Scheduled': 'Scheduled',
    'Wx Scheduled': 'Signed Contracts',
    'Permit Submitted': 'Recommended - Unsigned',
    'Permit Approved': 'Recommended - Unsigned'
}

def clean_contact_info(row) -> Series:
    row['PersonEmail'] = toSalesforceEmail(row['PersonEmail']) or nan
    row['Phone'] = toSalesforcePhone(row['Phone']) or nan
    return row

def vhi(data:DataFrame) -> DataFrame:
    # Removing unprocessable rows
    data = data[~data['VHI Unique Number'].isna()]
    data = data[~data['Contact: Email'].isna() | ~data['Contact: Phone'].isna()]
    
    # Rename columns into Salesforce Field
    data = data.rename(columns={
        'VHI Unique Number': 'ID_from_HPC__c',
        'Contact: Phone': 'Phone',
        'Contact: Email':'PersonEmail',
        'Audit Date & Time': 'HEA_Date_And_Time__c',
        'Stage':'StageName',
        'Health & Safety Issues': 'Health_Safety_Barrier__c',
        'Date of work': 'Weatherization_Date_Time__c',
        'Amount': 'Final_Contract_Amount__c'
    })

    # Combine street and city
    data['Street__c'] = data['Address'] + " " + data['Billing City']

    # Translate stage into stagename and wx status
    data['Weatherization_Status__c'] = nan
    data.loc[(data['StageName'] == 'Wx Scheduled'), 'Weatherization_Status__c'] = 'Scheduled'
    data.loc[(data['StageName'] == 'Wx Completed'), 'Weatherization_Status__c'] = 'Completed'
    data['Cancelation_Reason_s__c'] = nan
    data.loc[(data['Lead Vendor'] == 'ABCD'), 'Cancelation_Reason_s__c'] = 'Low Income'
    data.loc[(data['StageName'] == 'Canceled'), 'Cancelation_Reason_s__c'] = 'No Reason'
    data['StageName'] = data['StageName'].map(stageMapper)

    # Date and Time
    data['CloseDate'] = to_datetime(data['HEA_Date_And_Time__c'], errors='coerce').dt.strftime(
        '%Y-%m-%d'+'T'+'%H:%M:%S'+'.000-07:00')
    data['HEA_Date_And_Time__c'] = to_datetime(data['HEA_Date_And_Time__c'], errors='coerce').dt.strftime(
        '%Y-%m-%d'+'T'+'%H:%M:%S'+'.000-07:00')
    data['Weatherization_Date_Time__c'] = to_datetime(data['Weatherization_Date_Time__c'], errors='coerce').dt.strftime(
        '%Y-%m-%d'+'T'+'%H:%M:%S'+'.000-07:00')
    
    # first name and last name
    data['FirstName'] = data['Opportunity Name'].str.extract(r'^(.*?)\s(?:\S+\s)*\S+$')
    data['LastName'] = data['Opportunity Name'].str.extract(r'\s(.*)$')
    
    # Clean up contact info
    data = data.apply(clean_contact_info, axis=1)

    return data