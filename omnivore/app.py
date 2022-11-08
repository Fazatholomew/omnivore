from .utils.salesforce import SalesforceConnection
from dotenv import load_dotenv
from os import getenv

import neeeco
import homeworks

# Load environment variable from .env
load_dotenv()

class Blueprint:

    @staticmethod
    def run():
        sf = SalesforceConnection(username=getenv('EMAIL'), consumer_key=getenv('CONSUMER_KEY'), privatekey_file=getenv('PRIVATEKEY_FILE'))
        sf.get_salesforce_table()
    
    neeeco()
    homeworks()

