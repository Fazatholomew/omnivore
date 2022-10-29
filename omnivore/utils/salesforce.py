from simple_salesforce import Salesforce
from dotenv import load_dotenv
from os import getenv
from .constants import PERSON_ACCOUNT_ID

# Opportunity columns to fetch
OPPORTUNITY_COLUMNS = [
    'Id',
    'Street__c',
    'Unit__c',
    'City__c',
    'State__c',
    'Zipcode__c',
    'Name',
    'HEA_Date_And_Time__c',
    'CloseDate',
    'StageName',
    'Health_Safety_Barrier_Status__c',
    'Health_Safety_Barrier__c',
    'isVHEA__c',
    'Weatherization_Status__c',
    'Weatherization_Date_Time__c',
    'Contract_Amount__c',
    'Final_Contract_Amount__c',
    'AccountId',
    'RecordTypeId',
    'ID_from_HPC__c',
    'Owner_Renter__c',
    'All_In_Energy_ID__c',
    'Staff_acc__c',
    'Don_t_Omnivore__c',
    'Set_By__c',
]
# Account columns to fetch
ACCOUNT_COLUMNS = [
    'Id',
    'BillingStreet',
    'BillingCity',
    'BillingState',
    'BillingPostalCode',
    'FirstName',
    'LastName',
    'Phone',
    'PersonEmail',
    'Gas_Utility__c',
    'Electric_Utility__c',
    'RecordTypeId',
    'Owner_Renter__c',
    'All_In_Energy_ID__c',
    'Field_Staff__c',
]


class SalesforceConnection:
    '''
      Salesforce connection instance synchronously.
    '''

    def __init__(self, username, consumer_key, privatekey_file):
        self.sf = Salesforce(username, consumer_key=consumer_key, privatekey_file=privatekey_file)
        self.table = {}

    def get_salesforce_table(self):
        res = self.sf.query_all(f"SELECT {', '.join(ACCOUNT_COLUMNS)} FROM Account WHERE RecordTypeID = '{PERSON_ACCOUNT_ID}'")
        if res['done']:
          for account in res['records']:
            print(account['Id'])
