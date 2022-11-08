from .utils.salesforce import SalesforceConnection
from dotenv import load_dotenv
from os import getenv
from pickle import load

from .neeeco import neeeco
from .homeworks import homeworks

# Load environment variable from .env
load_dotenv()


class Blueprint:

    def __init__(self) -> None:
        if getenv('EMAIL'):
            self.sf = SalesforceConnection(username=getenv('EMAIL'), consumer_key=getenv(  # type:ignore
                'CONSUMER_KEY'), privatekey_file=getenv('PRIVATEKEY_FILE'))  # type:ignore
            self.sf.get_salesforce_table()

    def load_processed_rows(self, fileName = './processed_row') -> None:
      with open(fileName, 'rb') as file_blob:
        self.processed_rows = load(file_blob)

    @staticmethod
    def run():
        pass
