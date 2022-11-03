from pickle import load

def load_dummy_query_all(query):
  if len(query.split('FROM Account')) == 2:
    with open('./tests/utils/account', 'rb') as f:
      return load(f)

  if len(query.split('FROM Opportunity')) == 2:
    with open('./tests/utils/opportunity', 'rb') as f:
      return load(f)

if __name__ == '__main__':
  opps = {}
  for opp in load_dummy_query_all('dada FROM Opportunity fsfsf')['records']:
    if not opp['AccountId'] in opps:
      opps[opp['AccountId']] = []
    opps[opp['AccountId']].append(opp['Id'])
  print(opps)
  load_dummy_query_all('dada FROM Account fsfsf')
  