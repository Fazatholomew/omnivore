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
    Owner_Renter__c: str
    # Cambridge
    Primary_Heating_Fuel__c: str
    Description: str
    Number_of_Units_in_the_Building_Condo_As__c: str
    Number_of_Floors__c: str
    Square_Footage__c: str
    Building_Age__c: str
    Heating_System_Age__c: str
    Cooling_System_Age__c: str
    Hot_Water_Fuel__c: str
    Hot_Water_System_Type__c: str
    Existing_Solar__c: str
    Roof_Age__c: str
    Electrical_Panel_Capacity__c: str
    Number_of_Spaces__c: str
    Existing_EV_Charging__c: str
    Which_decarb_technologies_interest_you__c: str
    Map_Lot_ID_Cambridge_Property_Assessors__c: str
    Building_Envelope__c: str
    Relationship_to_Building__c: str
    Owner_Property_Management_Email__c: str
    Owner_Property_Management_Name__c: str
    Owner_Property_Management_Phone__c: str
    Did_you_find_your_consultation_helpful__c: str
    Were_your_advisors_helpful__c: str
    Open_feedback_what_should_we_know__c: str
    Billable_Time_Spent__c: str
    What_problem_s_are_you_facing_cambridge__c: str
    Consultation_Date__c: str
    What_is_known_about_cambridge__c: str
    Building_common_areas_or_cambridge__c: str
    Location_IOM__c: str
    Ventilation_system_for_indoor_cambridge__c: str
    Site_Visit_Date_date__c: str
    Milestone_1_Date_cambridge__c: str
    Milestone_2_Date_cambridge__c: str
    Milestone_3_Date_cambridge__c: str
    Milestone_4_Date_cambridge__c: str
    Expected_Follow_Up_Date__c: str
    Total_Square_Footage__c: str
    Cambridge_Data_Sorce__c: str


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
    Prefered_Lan__c: str
    PersonDoNotCall: bool


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
