from typing import TypedDict

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
    Don_t_Omnivore__c: bool
    Set_By__c: str
    tempId: str
    HPC__c: str
    CampaignId: str
    Cancelation_Reason_s__c: str
    Legacy_Post_Assessment_Notes__c: str

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


class Query(TypedDict, total=True):
    totalSize: int
    done: bool
    nextRecordsUrl: str
    records: list[Opportunity | Account]


class Record_Find_Info(TypedDict, total=True):
    acc: Account
    opps: list[Opportunity]

class Create(TypedDict, total=True):
  errors: list[str]
  id: str
  success: bool