from omnivore.app import Blueprint
from unittest.mock import patch, mock_open
from pandas import DataFrame
import pytest

@pytest.fixture()
def dataframe():
    # Load dummy in test
    return DataFrame(data={'Name': ['Jimmy Dummy', 'Testy Test'], 'HEA Status': ['Scheduled', 'Completed']})

@pytest.fixture()
def mocking_open():
  with patch('omnivore.app.open', mock_open()) as MockSf:
    print('mocking open')
    yield MockSf

def test_remove_processed_row(dataframe, mocking_open):
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

