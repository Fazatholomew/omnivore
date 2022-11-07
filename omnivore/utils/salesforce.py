from simple_salesforce import Salesforce
from dotenv import load_dotenv
from os import getenv
from .constants import PERSON_ACCOUNT_ID, HEA_ID, OPPS_RECORD_TYPE, CFP_ACCOUNT_ID
from .aux import extractId, toSalesforceEmail, toSalesforcePhone
from typing import TypedDict, Dict, cast

# from pickle import dump
# from random import sample

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


class Opportunity(TypedDict, total=False):
    Id: str
    Street__c: str
    Unit__c: str
    City__c: str
    State__c: str
    Zipcode__c: str
    Name: str
    HEA_Date_And_Time__c: str
    CloseDate: str
    StageName: str
    Health_Safety_Barrier_Status__c: str
    Health_Safety_Barrier__c: str
    isVHEA__c: str
    Weatherization_Status__c: str
    Weatherization_Date_Time__c: str
    Contract_Amount__c: str
    Final_Contract_Amount__c: str
    AccountId: str
    RecordTypeId: str
    ID_from_HPC__c: str
    Owner_Renter__c: str
    All_In_Energy_ID__c: str
    Staff_acc__c: str
    Don_t_Omnivore__c: str
    Set_By__c: str


class Account(TypedDict, total=False):
    Id: str
    BillingStreet: str
    BillingCity: str
    BillingState: str
    BillingPostalCode: str
    FirstName: str
    LastName: str
    Phone: str
    PersonEmail: str
    Gas_Utility__c: str
    Electric_Utility__c: str
    RecordTypeId: str
    Owner_Renter__c: str
    All_In_Energy_ID__c: str
    Field_Staff__c: str


class Query(TypedDict):
    totalSize: int
    done: bool
    nextRecordsUrl: str
    records: list[Opportunity | Account]


class Record_Find_Info(TypedDict):
    acc: Account
    opps: list[Opportunity]

class Create(TypedDict):
  errors: list[str]
  id: str
  success: bool




class SalesforceConnection:
    '''
      Salesforce connection instance synchronously.
    '''

    def __init__(self, username: str, consumer_key: str, privatekey_file: str):
        self.sf = Salesforce(username, consumer_key=consumer_key, privatekey_file=privatekey_file)
        # the key is Account ID. Value is a list with opportunity record. details
        self.accId_to_oppIds: Dict[str, list[str]] = {}
        self.email_to_accId: Dict[str, str] = {}  # the key is email. Value is Account ID assoiated with the email
        self.phone_to_accId: Dict[str, str] = {}  # the key is phone. Value is Account ID assoiated with the phone
        # the key could AIE ID or ID from HPC. Value is Opportunity ID assoiated with the id
        self.ids_to_oppId: Dict[str, str] = {}
        self.oppId_to_opp: Dict[str, Opportunity] = {}  # Where all opps are, indexed by Opportunity ID
        self.accId_to_acc: Dict[str, Account] = {}  # Where all Accounts are, indexed by Account ID

    def get_salesforce_table(self):
        '''
          Create multiple look up table that reflect current SF Database
        '''
        # Querying all Opportunities
        res: Query = cast(Query, self.sf.query_all(
            f"SELECT {', '.join(OPPORTUNITY_COLUMNS)} FROM Opportunity WHERE RecordTypeID IN ('{OPPS_RECORD_TYPE}')"))
        # Generating dummies
        # dummy_data = sample(res['records'], 15)
        # for opp in res['records']:
        #   if opp['Id'] == '0068Z00001YqFDhQAN' or opp['Id'] == '0068Z00001YqMLnQAN':
        #     dummy_data.append(opp)
        # res['records'] = dummy_data
        # with open('opportunity', 'wb') as opp_file:
        #   dump(res, opp_file)
        for opportunity in res['records']:
            opportunity = cast(Opportunity, opportunity)
            if not opportunity['AccountId'] in self.accId_to_oppIds:
                self.accId_to_oppIds[opportunity['AccountId']] = []
            self.accId_to_oppIds[opportunity['AccountId']].append(opportunity['Id'])
            self.ids_to_oppId[f"{opportunity['ID_from_HPC__c']}"] = opportunity['Id']
            # Extracting AIE ID
            aie_id = extractId(opportunity['All_In_Energy_ID__c'])
            if len(aie_id) > 0:
                self.ids_to_oppId[aie_id[0]] = opportunity['Id']
            self.oppId_to_opp[opportunity['Id']] = opportunity
        res: Query = cast(Query, self.sf.query_all(
            f"SELECT {', '.join(ACCOUNT_COLUMNS)} FROM Account WHERE RecordTypeID IN ('{PERSON_ACCOUNT_ID}', '{CFP_ACCOUNT_ID}')"))
        # Generating Dummies
        # accs = []
        # current_accs = {}
        # for acc in res['records']:
        #   current_accs[acc['Id']] = acc
        # for opp in dummy_data:
        #   if opp['AccountId'] in current_accs:
        #     accs.append(current_accs[opp['AccountId']])
        # res['records'] = accs
        # with open('account', 'wb') as opp_file:
        #   dump(res, opp_file)
        for account in res['records']:
            account = cast(Account, account)
            # Cleaning email and phone
            cleaned_email = toSalesforceEmail(account['PersonEmail'])
            if len(cleaned_email) > 0:
                self.email_to_accId[cleaned_email] = account['Id']

            cleaned_phone = toSalesforcePhone(account['Phone'])
            if len(cleaned_phone) > 0:
                self.phone_to_accId[cleaned_phone] = account['Id']
            self.accId_to_acc[account['Id']] = account

    def find_records(self, input_records: Record_Find_Info) -> list[Opportunity]:
        '''
          Match record in salesforce using Email, Phone, AIE ID, ID from HPC.
          The input has phone and email to recognize which account the opportunities belong.
        '''
        found_opps: list[Opportunity] = []
        for opp in input_records['opps']:
            # Search matched using AIE ID and ID from HPC
            # If found, store new value in found_opps and continue the loop
            if 'All_In_Energy_ID__c' in opp:
              aie_ids = extractId(opp['All_In_Energy_ID__c'])
              if len(aie_ids) > 0:
                aie_id = aie_ids[0]
                if aie_id in self.ids_to_oppId:
                  self.oppId_to_opp[self.ids_to_oppId[aie_id]].update(opp)
                  found_opps.append(self.oppId_to_opp[self.ids_to_oppId[aie_id]])
                  continue
            if 'ID_from_HPC__c' in opp and opp['ID_from_HPC__c'] in self.ids_to_oppId:
                self.oppId_to_opp[self.ids_to_oppId[opp['ID_from_HPC__c']]].update(opp)
                found_opps.append(self.oppId_to_opp[self.ids_to_oppId[opp['ID_from_HPC__c']]])
                continue
            # Search using Account ID
            if len(found_opps) > 0:
                # Check whether Opportunity already in SF
                account_id: str = found_opps[0]['AccountId']
                empty_oppIds: list[str] = [oppId for oppId in self.accId_to_oppIds[account_id]
                                           if not self.oppId_to_opp[oppId]['ID_from_HPC__c']]
                if len(empty_oppIds) > 0:
                    # Assign one of the opportunity into current row
                    current_oppId = empty_oppIds[0]
                    self.oppId_to_opp[current_oppId].update(opp)
                    found_opps.append(self.oppId_to_opp[current_oppId])
                    continue
                # This is new Opp, create a new one by flagging empty ID but with Account ID
                opp['AccountId'] = account_id
                found_opps.append(opp)
                continue

            # No Account ID yet, search using email and phone
            if 'PersonEmail' in input_records['acc']:
                # Search using Email
                if len(input_records['acc']['PersonEmail']) > 0:
                    if input_records['acc']['PersonEmail'] in self.email_to_accId:
                        # Check whether Opportunity already in SF
                        account_id: str = self.email_to_accId[input_records['acc']['PersonEmail']]
                        empty_oppIds: list[str] = [oppId for oppId in self.accId_to_oppIds[account_id]
                                                   if not self.oppId_to_opp[oppId]['ID_from_HPC__c']]
                        if len(empty_oppIds) > 0:
                            # Assign one of the opportunity into current row
                            current_oppId = empty_oppIds[0]
                            self.oppId_to_opp[current_oppId].update(opp)
                            found_opps.append(self.oppId_to_opp[current_oppId])
                            continue
                        # This is new Opp, create a new one by flagging empty ID but with Account ID
                        opp['AccountId'] = account_id
                        found_opps.append(opp)
                        continue

            if 'Phone' in input_records['acc']:
                # Search using Phone
                if len(input_records['acc']['Phone']) > 0:
                    if input_records['acc']['Phone'] in self.phone_to_accId:
                        # Check whether Opportunity already in SF
                        account_id: str = self.phone_to_accId[input_records['acc']['Phone']]
                        empty_oppIds: list[str] = [oppId for oppId in self.accId_to_oppIds[account_id]
                                                   if not self.oppId_to_opp[oppId]['ID_from_HPC__c']]
                        if len(empty_oppIds) > 0:
                            # Assign one of the opportunity into current row
                            current_oppId = empty_oppIds[0]
                            self.oppId_to_opp[current_oppId].update(opp)
                            found_opps.append(self.oppId_to_opp[current_oppId])
                            continue
                        # This is new Opp, create a new one by flagging empty ID but with Account ID
                        opp['AccountId'] = account_id
                        found_opps.append(opp)
                        continue
            # Account Not found break
            break
        if len(found_opps) == 0:
          # Account not found create a new account
          res: Create = cast(Create, self.sf.Account.create(input_records['acc'])) # type:ignore
          if res['success']:
            for opp in input_records['opps']:
              opp['AccountId'] = res['id']
              found_opps.append(opp)
        return found_opps