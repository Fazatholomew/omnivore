import pytest
from omnivore.utils.salesforce import (
    SalesforceConnection,
    Record_Find_Info,
    Account,
    Opportunity,
    Create,
)
from unittest.mock import patch
from .salesforce_data import load_dummy_query_all


@pytest.fixture()
def sf():
    # Load dummy in test
    with patch("omnivore.utils.salesforce.Salesforce") as MockSf:
        MockSf.return_value.query_all.side_effect = load_dummy_query_all
        sf = SalesforceConnection("", "", "")
        sf.get_salesforce_table()
        yield sf


# Get Salesforce Table


def test_get_salesforce_table(sf):
    # Test Email to Account ID
    assert sf.email_to_accId["davidmcmenimen@hotmail.co"] == "0013i00002YKIiNAAX"
    # Test Phone to Account ID
    assert sf.phone_to_accId["6172178302"] == "0018Z00002ifJUkQAM"
    # Test 2 opps in 1 account
    assert len(sf.accId_to_oppIds["0018Z00002ifJUkQAM"]) == 2
    # Test account presents
    assert sf.accId_to_acc["0018Z00002ifJUkQAM"]["Id"] == "0018Z00002ifJUkQAM"
    # Test Opp presents
    assert sf.oppId_to_opp["0068Z00001YqFDhQAN"]["Id"] == "0068Z00001YqFDhQAN"
    # Test if opportunity has data
    assert sf.accId_to_oppIds["0018Z00002ifJUkQAM"][0] == "0068Z00001YqFDhQAN"
    # Test All In Energy ID
    assert sf.ids_to_oppId["0063i00000ArTc7AAF"] == "0063i00000ArTc7AAF"
    # Test HPC ID
    assert sf.ids_to_oppId["260798493"] == "0063i00000ArTc7AAF"


# Find


def test_find_records_ids_search(sf: SalesforceConnection):
    # Find records using ID From HPC and AIE ID
    input_data = Record_Find_Info(
        acc=Account(LastName="Test"),
        opps=[
            Opportunity(
                All_In_Energy_ID__c="<|>0063i00000CwA9NAAV<|>", CloseDate="date"
            ),
            Opportunity(ID_from_HPC__c="259292987", CloseDate="date"),
        ],
    )
    output_data = sf.find_records(input_data)
    assert output_data[0]["Id"] == "0063i00000CwA9NAAV"
    assert output_data[1]["Id"] == "0063i00000DEbCsAAL"


def test_find_records_phone_search(sf: SalesforceConnection):
    # Find records using Phone and create a new Opp
    input_data = Record_Find_Info(
        acc=Account(LastName="Test", Phone="6172178302"),
        opps=[
            Opportunity(CloseDate="date"),
            Opportunity(CloseDate="date"),
            Opportunity(CloseDate="date"),
        ],
    )
    output_data = sf.find_records(input_data)
    print(output_data)
    assert "Id" not in output_data[0]
    assert output_data[1]["AccountId"] == "0018Z00002ifJUkQAM"


def test_find_records_email_search(sf: SalesforceConnection):
    # Find records using email and create new opp
    # Set the HPC ID
    input_data = Record_Find_Info(
        acc=Account(LastName="Test", PersonEmail="nolviagutierrez1977@gmail.com"),
        opps=[
            Opportunity(CloseDate="date", ID_from_HPC__c="testID"),
            Opportunity(CloseDate="date"),
        ],
    )
    output_data = sf.find_records(input_data)
    assert output_data[0]["AccountId"] == "0013i00002YLCJ0AAP"
    assert output_data[0]["Id"] == "0063i00000EcBUeAAN"
    assert sf.oppId_to_opp["0063i00000EcBUeAAN"]["ID_from_HPC__c"] == "testID"
    assert "Id" not in output_data[1]
    assert output_data[1]["AccountId"] == "0013i00002YLCJ0AAP"


def test_find_records_no_Account():
    # No Account found. create a new Account
    with patch("omnivore.utils.salesforce.Salesforce") as MockSf:
        MockSf.return_value.query_all.side_effect = load_dummy_query_all
        MockSf.return_value.Account.create.return_value = Create(
            errors=[], id="new Account Id", success=True
        )
        sf = SalesforceConnection("", "", "")
        sf.get_salesforce_table()
        input_data = Record_Find_Info(
            acc=Account(LastName="Test", PersonEmail="jimmy@gmial.com"),
            opps=[
                Opportunity(CloseDate="date", ID_from_HPC__c="testID"),
                Opportunity(CloseDate="date"),
            ],
        )
        output_data = sf.find_records(input_data)
        input_data["acc"].update({"RecordTypeId": "0123i000000YGesAAG"})
        MockSf.return_value.Account.create.assert_called_with(input_data["acc"])
        assert output_data[0]["AccountId"] == "new Account Id"
        assert output_data[1]["AccountId"] == "new Account Id"
        assert "Id" not in output_data[0]
        assert "Id" not in output_data[1]
