from simple_salesforce import Salesforce
from dotenv import load_dotenv
from os import getenv

load_dotenv('./.env.staging')
sf = Salesforce(username=getenv('EMAIL'), consumer_key=getenv(  # type:ignore
                'CONSUMER_KEY'), privatekey_file=getenv('PRIVATEKEY_FILE'), domain='test')
res = sf.query_all('SELECT Id FROM Opportunity')
for opp in res['records']:
  sf.Opportunity.delete(opp['Id']) #type:ignore
res = sf.query_all('SELECT Id FROM Account')
for acc in res['records']:
  sf.Account.delete(acc['Id']) #type:ignore