from simple_salesforce import Salesforce
from dotenv import load_dotenv
from os import getenv

# Load .env variable
load_dotenv()

print(getenv('EMAIL'))
sf = Salesforce(username=getenv('EMAIL'), consumer_key=getenv('CONSUMER_KEY'), privatekey_file=getenv('PRIVATEKEY_FILE'))
print(sf.query("SELECT Id, Email FROM Lead WHERE FirstName = 'Jimmy'"))