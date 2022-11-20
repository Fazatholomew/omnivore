from omnivore.app import Blueprint
from .utils.salesforce_data import load_dummy_query_all
from omnivore.utils.types import Record_Find_Info, Account, Opportunity
from omnivore.utils.constants import NEEECO_ACCID
from unittest.mock import patch, mock_open
from pandas import DataFrame
import pytest

def mock_sf_update(ID, data):
  if ID == None or ID == '':
    raise Exception('No ID')
  if 'tempId' in data:
    raise Exception('Shouldn\'t have tempId')
  return 204

def mock_sf_create(data):
  if 'tempId' in data:
    raise Exception('Shouldn\'t have tempId')
  return {'success': True}

@pytest.fixture()
def dataframe():
    # Load dummy in test
    return DataFrame(data={'Name': ['Jimmy Dummy', 'Testy Test'], 'HEA Status': ['Scheduled', 'Completed']})

@pytest.fixture()
def mocking_open():
  with patch('omnivore.app.open', mock_open()) as MockSf:
    yield MockSf

@pytest.fixture()
def sf():
    # Load dummy in test
    with patch('omnivore.utils.salesforce.Salesforce') as MockSf:
        MockSf.return_value.query_all.side_effect = load_dummy_query_all
        MockSf.return_value.Account.create.return_value = 'new account'
        MockSf.return_value.Opportunity.update.side_effect = mock_sf_update
        MockSf.return_value.Opportunity.create.side_effect = mock_sf_create
        with patch('omnivore.app.open', mock_open()):
          with patch('omnivore.app.load') as mockLoad:
            dummy_processed = set()
            dataframe = DataFrame(data={'Name': ['Jimmy Dummy', 'Testy Test'], 'HEA Status': ['Scheduled', 'Completed']}) 
            dummy_processed.add(''.join(dataframe.values.tolist()[0]).lower())
            mockLoad.return_value = dummy_processed
            yield MockSf

def test_remove_processed_row(sf, dataframe, mocking_open):
    with patch('omnivore.app.load') as MockSf:
        dummy_processed = set()
        dummy_processed.add(''.join(dataframe.values.tolist()[0]).lower())
        MockSf.return_value = dummy_processed
        omni = Blueprint()
        MockSf.assert_called_once()
        mocking_open.assert_called_once()
        # Test processed row 
        assert len(omni.processed_rows) == 1
        # Test Removing row that already processed
        assert len(dataframe) == 2
        removed = omni.remove_already_processed_row(dataframe)
        assert 'tempId' in removed.columns
        assert len(removed) == 1
        assert removed.iloc[0]['Name'] == 'Testy Test'
        # if row changes, keep it
        dataframe.iloc[0]['HEA Status'] = 'Completed'
        not_removed = omni.remove_already_processed_row(dataframe)
        assert len(not_removed) == 2

@pytest.mark.asyncio
async def test_upload_to_salesforce(sf):
  omni = Blueprint()
  # Test don't omnivore
  data: list[Record_Find_Info] = [Record_Find_Info(acc=Account(PersonEmail='aldoglean@gmail.com'), opps=[Opportunity(ID_from_HPC__c='259292987', Don_t_Omnivore__c=True, tempId='test test test')])]
  await omni.start_upload_to_salesforce(data, NEEECO_ACCID)
  assert 'test test test' in omni.processed_rows
  assert not sf.return_value.called
  # Test updating
  data: list[Record_Find_Info] = [Record_Find_Info(acc=Account(PersonEmail='aldoglean@gmail.com'), opps=[Opportunity(ID_from_HPC__c='259292987', tempId='test test', StageName='Completed', Don_t_Omnivore__c=False)])]
  await omni.start_upload_to_salesforce(data, NEEECO_ACCID)
  assert sf.return_value.Opportunity.update.call_args_list[0][0][0] == '0063i00000DEbCsAAL'
  assert sf.return_value.Opportunity.update.call_args_list[0][0][1]['StageName'] == 'Completed'
  # Test creating
  data: list[Record_Find_Info] = [Record_Find_Info(acc=Account(PersonEmail='aldoglean@gmail.com'), opps=[Opportunity(ID_from_HPC__c='259292911', tempId='test test', StageName='Completed', Don_t_Omnivore__c=False)])]
  await omni.start_upload_to_salesforce(data, NEEECO_ACCID)
  assert sf.return_value.Opportunity.create.call_args_list[0][0][0]['StageName'] == 'Completed'
  assert 'test test' in omni.processed_rows

def test_saving_processed_row():
  with patch('omnivore.utils.salesforce.Salesforce'):
    with patch('omnivore.app.open', mock_open()) as mocked_open:
      with patch('omnivore.app.load') as mock_load:
        with patch('omnivore.app.dump') as mock_dump:
          mock_load.return_value = set()
          omni = Blueprint()
          omni.processed_rows.add('test test')
          omni.save_processed_rows()
          mocked_open.assert_called_with('./processed_row', 'wb')
          mock_dump.assert_called_with(set(['test test']), mocked_open.return_value)
