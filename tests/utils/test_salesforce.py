import pytest
from omnivore.utils.salesforce import SalesforceConnection
from unittest.mock import Mock, patch

def generate_sf_query_result(query):
  if 'Account' in query:
    return {}

  if 'Opportunity' in query:
    return {}

def test_get_salesforce_table():
  with patch('omnivore.utils.salesforce.Salesforce') as MockSf:
    MockSf.return_value.query_all.sideeffect = generate_sf_query_result
    sf = SalesforceConnection('', '', '') 
    sf.get_salesforce_table()
    MockSf.return_value.query_all.assert_called_with(ANY, ANY)