from os import getenv
from pickle import load, dump
from typing import cast, Any
from pandas import DataFrame

import pandas as pd
from dotenv import load_dotenv

from .homeworks.homeworks import homeworks
from .neeeco.neeeco import neeeco
from .utils.salesforce import SalesforceConnection

# Load environment variable from .env
load_dotenv()


class Blueprint:
    def __init__(self) -> None:
        if getenv('EMAIL'):
            self.sf = SalesforceConnection(username=getenv('EMAIL'), consumer_key=getenv(  # type:ignore
                'CONSUMER_KEY'), privatekey_file=getenv('PRIVATEKEY_FILE'))  # type:ignore
            self.sf.get_salesforce_table()
        self.load_processed_rows()

    def load_processed_rows(self, fileName = './processed_row') -> None:
      '''
        Load already processed rows from pickled file
      '''
      with open(fileName, 'rb') as file_blob:
        self.processed_rows = cast(set[str] ,load(file_blob))
    
    def save_processed_rows(self, fileName = './processed_row') -> None:
      '''
        Save processed rows into pickled file
      '''
      with open(fileName, 'wb') as file_blob:
        dump(self.processed_rows, file_blob)

    def remove_already_processed_row(self, data: DataFrame) -> DataFrame:
      '''
        Remove rows that already processed and no changes occures
      '''
      return data[data[data.columns].T.agg(''.join).str.lower().isin(self.processed_rows)].copy()

    def add_row_to_processed_row(self, data: list[Any]) -> None:
      '''
        Add already processed row into the set
      '''
      self.processed_rows.add(''.join(data).lower())
    
    @staticmethod
    def run():
        pass
