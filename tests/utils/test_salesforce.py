import pytest
from omnivore.utils.salesforce import SalesforceConnection
from unittest.mock import Mock, patch, ANY
from .salesforce_data import load_dummy_query_all

@pytest.fixture()
def sf():
  # Get salesforce table should load dummy in test
  with patch('omnivore.utils.salesforce.Salesforce') as MockSf:
    MockSf.return_value.query_all.side_effect = load_dummy_query_all
    sf = SalesforceConnection('', '', '') 
    sf.get_salesforce_table()
    yield sf

# Get Salesforce Table
def test_aieId_table(sf):
  assert sf.aieId_to_oppId['<|>0063i00000DD4EkAAL<|>'] == '0063i00000DD4EkAAL'
