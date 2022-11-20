import pytest
from unittest.mock import patch, mock_open
from omnivore.app import Blueprint
from .neeeco.neeeco_data import output_data
from datetime import datetime

@pytest.fixture
def start_app():
  '''
    Initialize app as if the script is executed. Still use mock for reading and
    writing
  '''
  with patch('omnivore.app.open', mock_open()):
    with patch('omnivore.app.load') as mock_load:
      with patch('omnivore.app.dump'):
        now = datetime.now()
        mock_load.return_value = set() # Empty processed row so in the beginning everything is processed
        omni = Blueprint()
        yield omni
        # Delete created records
        opps = omni.sf.sf.query_all(f"SELECT Id FROM Opportunity WHERE CreatedDate > {now.astimezone().strftime('%Y-%m-%dT%H:%M:%S.000%z')}")
        accs = omni.sf.sf.query_all(f"SELECT Id FROM Account WHERE CreatedDate > {now.astimezone().strftime('%Y-%m-%dT%H:%M:%S.000%z')}")
        for opp in opps['records']:
          omni.sf.sf.Opportunity.delete(opp['Id']) #type:ignore
        for acc in accs['records']:
          omni.sf.sf.Account.delete(acc['Id']) #type:ignore

@pytest.mark.staging
def test_creating_records(start_app):
  '''
    Test integration with salesforce by creating and querying record.
    Assume empty sandbox
  '''
  res = start_app.sf.sf.Account.create({
    'LastName': 'test test'
  })
  print(res['id'])
  assert False
