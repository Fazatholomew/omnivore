import pytest
from unittest.mock import patch, mock_open
from omnivore.app import Blueprint
from omnivore.utils.constants import HEA_ID
from .neeeco.neeeco_data import output_data
from datetime import datetime


@pytest.fixture
def init_app():
    '''
      Initialize app as if the script is executed. Still use mock for reading and
      writing
    '''
    with patch('omnivore.app.open', mock_open()):
        with patch('omnivore.app.load') as mock_load:
            with patch('omnivore.app.dump'):
                now = datetime.now()
                mock_load.return_value = set()  # Empty processed row so in the beginning everything is processed
                omni = Blueprint()
                yield omni
                # Delete created records
                opps = omni.sf.sf.query_all(
                    f"SELECT Id FROM Opportunity WHERE CreatedDate > {now.astimezone().strftime('%Y-%m-%dT%H:%M:%S.000%z')}")
                accs = omni.sf.sf.query_all(
                    f"SELECT Id FROM Account WHERE CreatedDate > {now.astimezone().strftime('%Y-%m-%dT%H:%M:%S.000%z')}")
                for opp in opps['records']:
                    omni.sf.sf.Opportunity.delete(opp['Id'])  # type:ignore
                for acc in accs['records']:
                    omni.sf.sf.Account.delete(acc['Id'])  # type:ignore


@pytest.mark.staging
def test_connecting_to_salesforce(init_app):
    '''
      Test integration with salesforce by creating and querying record.
      Assume empty sandbox
    '''
    # creating Account
    res = init_app.sf.sf.Account.create({
        'LastName': 'test test'
    })
    assert res['success']
    # creating opp
    res_opp = init_app.sf.sf.Opportunity.create({
        'CloseDate': datetime.now().astimezone().strftime('%Y-%m-%dT%H:%M:%S.000%z'),
        'RecordTypeId': HEA_ID,
        'AccountId': res['id'],
        'Name': 'Test opportunity',
        'StageName': 'Scheduled'
    })
    assert res_opp['success']
    # querying
    res_query = init_app.sf.sf.query_all(f"SELECT Id, AccountId from Opportunity WHERE RecordTypeId = '{HEA_ID}'")
    assert res_query['records'][0]['Id'] == res_opp['id']
    assert res_query['records'][0]['AccountId'] == res['id']


@pytest.mark.staging
def test_duplicates_account(init_app: Blueprint):
    '''
      Testing handling makin duplicate account
    '''
    # Create account
    created_account = init_app.sf.sf.Account.create(
        {'LastName': 'Jimmy', 'PersonEmail': 'test@gmail.com'})  # type:ignore
    found_opp = init_app.sf.find_records({
        'acc': {'LastName': 'Jimmy', 'PersonEmail': 'test@gmail.com'},
        'opps': [{
            'Name': 'Test Test'
        }]
    })
    assert len(found_opp) == 1
    assert found_opp[0]['AccountId'] == created_account['id']


@pytest.mark.staging
def test_app_using_neeeco(init_app):
    '''
      Testing whole app functionility including:
      - Connecting to SF and query current Database
      - Fetch Data from GAS
      - Process data for specific HPC (Neeeco)
      - Upload the data to SF
    '''
    init_app.run_neeeco()
    unique_ids = output_data['ID_from_HPC__c'].unique().tolist()
    joined = "', '".join(unique_ids)
    res = init_app.sf.sf.query_all(f"SELECT ID_from_HPC__c from Opportunity WHERE ID_from_HPC__c IN ('{joined}')")
    assert len(res['records']) == len(unique_ids)
    for opp in res['records']:
        assert opp['ID_from_HPC__c'] in unique_ids
