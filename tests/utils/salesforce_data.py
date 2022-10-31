from pickle import load

def load_dummy_query_all(query):
  if 'Account' in query:
    with open('./tests/utils/account') as f:
      return load(f)

  if 'Opportunity' in query:
    with open('./tests/utils/opportunity') as f:
      return load(f)