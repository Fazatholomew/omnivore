import pytest
from omnivore.utils.salesforce import SalesforceConnection
from unittest.mock import Mock, patch
from .salesforce_data import load_dummy_query_all

def test_get_salesforce_table():
  with patch('omnivore.utils.salesforce.Salesforce') as MockSf:
    MockSf.return_value.query_all.sideeffect = load_dummy_query_all
    sf = SalesforceConnection('', '', '') 
    sf.get_salesforce_table()
    MockSf.return_value.query_all.assert_called_with(ANY, ANY)