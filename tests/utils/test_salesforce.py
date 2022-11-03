import pytest
from omnivore.utils.salesforce import SalesforceConnection
from unittest.mock import Mock, patch, ANY
from .salesforce_data import load_dummy_query_all

@pytest.fixture()
def sf():
  # Load dummy in test
  with patch('omnivore.utils.salesforce.Salesforce') as MockSf:
    MockSf.return_value.query_all.side_effect = load_dummy_query_all
    sf = SalesforceConnection('', '', '') 
    sf.get_salesforce_table()
    yield sf

# Get Salesforce Table
def test_get_salesforce_table(sf):
  # Test Email to Account ID
  assert sf.email_to_accId['davidmcmenimen@hotmail.co'] == '0013i00002YKIiNAAX'
  # Test Phone to Account ID
  assert sf.phone_to_accId['6172178302'] == '0018Z00002ifJUkQAM' 
  # Test 2 opps in 1 account      
  assert len(sf.accId_to_oppIds['0018Z00002ifJUkQAM']) == 2
  # Test if opportunity has data
  assert sf.accId_to_oppIds['0018Z00002ifJUkQAM'][0]['Id'] == '0068Z00001YqFDhQAN' 
  # Test All In Energy ID
  assert sf.ids_to_oppId['0063i00000ArTc7AAF'] == '0063i00000ArTc7AAF'
  # Test HPC ID
  assert sf.ids_to_oppId['260798493'] == '0063i00000ArTc7AAF'

