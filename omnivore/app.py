from os import getenv
from pickle import load, dump
from typing import cast
from pandas import DataFrame, read_csv, isna
from dotenv import load_dotenv

from .homeworks.homeworks import homeworks
from .neeeco.neeeco import neeeco
from .utils.salesforce import SalesforceConnection, Create
from .utils.aux import to_account_and_opportunities, to_sf_payload, find_cfp_campaign
from .utils.types import Record_Find_Info
from .utils.constants import NEEECO_ACCID, OPPORTUNITY_COLUMNS, HEA_ID, CFP_OPP_ID

from asyncio import run, gather, get_event_loop

# Load environment variable from .env
if getenv('ENV') == 'production':
    load_dotenv('.env.production')

if not getenv('ENV'):
  load_dotenv()


class Blueprint:
    def __init__(self) -> None:
        if getenv('EMAIL') or getenv('ENV') == 'test':
            self.sf = SalesforceConnection(username=getenv('EMAIL'), consumer_key=getenv(  # type:ignore
                'CONSUMER_KEY'), privatekey_file=getenv('PRIVATEKEY_FILE'))  # type:ignore
            self.sf.get_salesforce_table()
        self.load_processed_rows()

    def load_processed_rows(self, fileName='./processed_row') -> None:
        '''
          Load already processed rows from pickled file
        '''
        try:
            with open(fileName, 'rb') as file_blob:
                self.processed_rows = cast(set[str], load(file_blob))
        except FileNotFoundError:
            self.processed_rows = set()

    def save_processed_rows(self, fileName='./processed_row') -> None:
        '''
          Save processed rows into pickled file
        '''
        with open(fileName, 'wb') as file_blob:
            dump(self.processed_rows, file_blob)

    def remove_already_processed_row(self, data: DataFrame) -> DataFrame:
        '''
          Remove rows that already processed and no changes occures
          Add temp id for processed row detection
        '''
        result = data.copy()
        result['tempId'] = result.fillna('').T.agg(''.join).str.lower()
        return result[~result['tempId'].isin(self.processed_rows)].copy()

    def upload_to_salesforce(self, data: Record_Find_Info, HPC_ID):
        '''
          Find and match opportunity then upload them
        '''
        if len(data['opps']) == 0:
          # No need to process if empty opp
          return
        found_records = self.sf.find_records(data)
        for opp in found_records:
            # Remove and keep tempId for processed row
            processed_row_id = opp.pop('tempId')
            if 'Don_t_Omnivore__c' in opp:
              # Don't omnivore is flagged
                if opp['Don_t_Omnivore__c']:
                    self.processed_rows.add(processed_row_id)
                    continue
            opp['HPC__c'] = HPC_ID
            opp['CampaignId'] = find_cfp_campaign(opp)
            opp['RecordTypeId'] = CFP_OPP_ID if opp['CampaignId'] else HEA_ID
            if 'Id' in opp:
                if len(opp['Id']) > 3:
                    try:
                        payload = to_sf_payload(opp, 'Opportunity')
                        res = self.sf.sf.Opportunity.update(opp['Id'], payload)  # type:ignore
                        if cast(int, res) > 200:
                            self.processed_rows.add(processed_row_id)
                            # Reporting
                    except Exception as err:
                        if (getenv('ENV') == 'staging'):
                          print(opp)
                          raise err
                        continue
            else:
                try:
                    payload = to_sf_payload(opp, 'Opportunity')
                    res: Create = self.sf.sf.Opportunity.create(payload)  # type:ignore
                    if res['success']:
                        self.processed_rows.add(processed_row_id)
                        # Reporting
                except Exception as err:
                    if (getenv('ENV') == 'staging'):
                      print(opp)
                      raise err
                    continue

    async def start_upload_to_salesforce(self, data: list[Record_Find_Info], HPC_ID: str) -> None:
        '''
          Start processing and uploading each account and opps asyncronously
        '''
        loop = get_event_loop()
        await gather(*[loop.run_in_executor(None, self.upload_to_salesforce, row, HPC_ID) for row in data])

    def run_neeeco(self) -> None:
        '''
          Run neeeco process
        '''
        raw_data = read_csv(cast(str, getenv('NEEECO_DATA_URL')), dtype='object')
        wx_data = read_csv(cast(str, getenv('NEEECO_WX_DATA_URL')), dtype='object')
        processed_row_removed = self.remove_already_processed_row(raw_data)
        processed_row = neeeco(processed_row_removed, wx_data)
        grouped_opps = to_account_and_opportunities(processed_row)
        run(self.start_upload_to_salesforce(grouped_opps, NEEECO_ACCID))
    
    def run(self) -> None:
      self.run_neeeco()
      self.save_processed_rows()
