from os import getenv

import pandas as pd
from dotenv import load_dotenv

from .homeworks.homeworks import homeworks
from .neeeco.neeeco import neeeco
from .utils.salesforce import SalesforceConnection

# Load environment variable from .env
load_dotenv()

class Blueprint:

    @staticmethod
    def run():
        sf = SalesforceConnection(username=getenv('EMAIL'), consumer_key=getenv('CONSUMER_KEY'), privatekey_file=getenv('PRIVATEKEY_FILE'))
        sf.get_salesforce_table()

        neeeco_input = pd.read_csv('Neeeco Input.csv')
        neeeco_wx_input = pd.read_csv('Neeeco Wx Input.csv')
        neeeco(neeeco_input,neeeco_wx_input)

        homeworks_input = pd.read_csv('/content/CFP Communities Report - Homeworks Input.csv')
        homeworks(homeworks_input)