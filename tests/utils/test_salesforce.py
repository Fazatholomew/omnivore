import pytest
from omnivore.utils.salesforce import SalesforceConnection
from unittest.mock import Mock

def generate_sf_query_result(query):
  if 'Account' in query:
    return {}

  if 'Opportunity' in query:
    return {}

def test_get_salesforce_table():
  sf = SalesforceConnection()
  sf.sf.query_all = Mock(side_effect=generate_sf_query_result)
  sf.get_salesforce_table()