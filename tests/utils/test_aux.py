from omnivore.utils.aux import toSalesforcePhone

def test_toSalesforcePhone():
  # Test extracting number
  clean = toSalesforcePhone('askdfbashdfhsda123dsfasdf444asdfasdf---==\'as4567')
  assert clean == '234444567'

  # Test to remove area code
  no_one = toSalesforcePhone('+1 917 333 3214')
  assert no_one == '9173333214'

  # Test only return 1 phone number that consists of 10 digits
  only_10 = toSalesforcePhone('+1 917 333 3214 +1 917 333 3214 +1 917 333 3214')
  assert len(only_10) == 10

  # Error prove if the input is not string
  nothing = toSalesforcePhone(None)
  assert nothing == ''
  
  # Error prove if the input is not string
  nothing = toSalesforcePhone({})
  assert nothing == ''

  # Error prove if the input is not string
  nothing = toSalesforcePhone([])
  assert nothing == ''

  # Convert to string
  phone_from_number = toSalesforcePhone(9123348123)
  assert phone_from_number == '9123348123'
