from os import getenv
from pickle import load, dump
from typing import cast
from pandas import DataFrame, read_csv
from asyncio import run, gather, get_event_loop
from simple_salesforce.exceptions import SalesforceMalformedRequest
import traceback
import logging
import time
from dotenv import load_dotenv

import sys
from pathlib import Path
# Add the project's root directory to the system path
sys.path.append(str(Path(__file__).resolve().parents[1]))

# Now you can import the modules using absolute import
from omnivore.homeworks.homeworks import homeworks, rename_and_merge
from omnivore.neeeco.neeeco import neeeco
from omnivore.vhi.vhi import vhi
from omnivore.utils.salesforce import SalesforceConnection, Create
from omnivore.utils.aux import to_account_and_opportunities, to_sf_payload, find_cfp_campaign
from omnivore.utils.types import Record_Find_Info
from omnivore.utils.constants import NEEECO_ACCID, HEA_ID, CFP_OPP_ID, HOMEWORKS_ACCID, VHI_ACCID

logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s - %(levelname)s - %(message)s",
    filename="logger.log",
    filemode="w",
)

# Create a logger object
logger = logging.getLogger()

# Remove existing handlers to avoid duplication
for handler in logger.handlers[:]:
    logger.removeHandler(handler)

# Create a file handler and set its level
file_handler = logging.FileHandler("logger.log")
file_handler.setLevel(logging.DEBUG)

# Create a formatter and set it for the file handler
formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
file_handler.setFormatter(formatter)

# Add the file handler to the logger
logger.addHandler(file_handler)

start_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())

class Blueprint:
    def __init__(self) -> None:
        if getenv('EMAIL') or getenv('ENV') == 'test':
            logger.debug('Load Database from SF')
            self.sf = SalesforceConnection(username=getenv('EMAIL'), consumer_key=getenv(  # type:ignore
                'CONSUMER_KEY'), privatekey_file=getenv('PRIVATEKEY_FILE'))  # type:ignore
            self.sf.get_salesforce_table()
        self.load_processed_rows()
        logger.debug('Finsihed loading Database from SF')

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
            processed_row_id = opp.pop('tempId') if 'tempId' in opp else opp['ID_from_HPC__c']
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
                    payload = to_sf_payload(opp, 'Opportunity')
                    current_id = payload.pop('Id')
                    try:
                        res = self.sf.sf.Opportunity.update(current_id, payload)  # type:ignore
                        if cast(int, res) > 200:
                            self.processed_rows.add(processed_row_id)
                            # Reporting
                    except SalesforceMalformedRequest as err:
                        if (err.content[0]['errorCode'] == 'FIELD_CUSTOM_VALIDATION_EXCEPTION' and 'cancelation' in err.content[0]['message']):
                        # Canceled stage needs a cancelation reason
                          try:
                              payload['Cancelation_Reason_s__c'] = 'No Reason' # Default reason
                              res = self.sf.sf.Opportunity.create(payload)  # type:ignore
                              if cast(int, res) > 200:
                                  self.processed_rows.add(processed_row_id)
                          except Exception as e:
                              if (getenv('ENV') == 'staging'):
                                  logger.error(payload)
                                  logger.error('failed to create after cancelation reason')
                                  logger.error(e)
                                  raise e
                              logger.error(traceback.format_exc())
                              logger.error(e) 
                    except Exception as err:
                        if (getenv('ENV') == 'staging'):
                            logger.error(payload)
                            logger.error('failed to update')
                            logger.error(err)
                            raise err
                        logger.error(traceback.format_exc())
                        logger.error(err)
                        continue
            else:
                payload = to_sf_payload(opp, 'Opportunity')
                try:
                    res: Create = self.sf.sf.Opportunity.create(payload)  # type:ignore
                    if res['success']:
                        self.processed_rows.add(processed_row_id)
                        # Reporting
                except SalesforceMalformedRequest as err:
                    if (err.content[0]['errorCode'] == 'DUPLICATE_VALUE'):
                        maybe_id = err.content[0]['message'].split('id: ')
                        if len(maybe_id) == 2:
                            try:
                                res = self.sf.sf.Opportunity.update(maybe_id[1], payload)  # type:ignore
                                if cast(int, res) > 200:
                                    self.processed_rows.add(processed_row_id)
                            except Exception as e:
                                if (getenv('ENV') == 'staging'):
                                    logger.error(payload)
                                    logger.error('failed to update after create')
                                    logger.error(e)
                                    raise e
                                logger.error(traceback.format_exc())
                                logger.error(e)
                            
                    if (err.content[0]['errorCode'] == 'FIELD_CUSTOM_VALIDATION_EXCEPTION' and 'cancelation' in err.content[0]['message']):
                        # Canceled stage needs a cancelation reason
                        try:
                            payload['Cancelation_Reason_s__c'] = 'No Reason' # Default reason
                            res = self.sf.sf.Opportunity.create(payload)  # type:ignore
                            if cast(int, res) > 200:
                                self.processed_rows.add(processed_row_id)
                        except Exception as e:
                            if (getenv('ENV') == 'staging'):
                                logger.error(payload)
                                logger.error('failed to create after cancelation reason')
                                logger.error(e)
                                raise e
                            logger.error(traceback.format_exc())
                            logger.error(e)
                    if (getenv('ENV') == 'staging'):
                        logger.error(payload)
                        logger.error('failed to update after create')
                        logger.error(err)
                        raise err
                    logger.error(traceback.format_exc())
                    logger.error(err)
                      
                except Exception as e:
                    logger.error('error creating')
                    if (getenv('ENV') == 'staging'):
                        logger.error(payload)
                        logger.error('failed to create')
                        logger.error(e)
                        raise e
                    logger.error(traceback.format_exc())
                    logger.error(e)
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
        try:
          raw_data = read_csv(cast(str, getenv('NEEECO_DATA_URL')), dtype='object')
          wx_data = read_csv(cast(str, getenv('NEEECO_WX_DATA_URL')), dtype='object')
          processed_row_removed = self.remove_already_processed_row(raw_data)
          processed_row = neeeco(processed_row_removed, wx_data)
          grouped_opps = to_account_and_opportunities(processed_row)
          run(self.start_upload_to_salesforce(grouped_opps, NEEECO_ACCID))
        except Exception as e:
          logger.error("Error in Neeeco process.")
          logger.error(traceback.format_exc())
          logger.error(e)


    def run_homeworks(self) -> None:
        '''
          Run Homeworks process
        '''
        try:
          new_data = read_csv(cast(str, getenv('HOMEWORKS_DATA_URL')), dtype='object')
          old_data = read_csv(cast(str, getenv('HOMEWORKS_COMPLETED_DATA_URL')), dtype='object')
          homeworks_output = rename_and_merge(old_data, new_data)
          processed_row_removed = self.remove_already_processed_row(homeworks_output)
          processed_row = homeworks(processed_row_removed)
          grouped_opps = to_account_and_opportunities(processed_row)
          run(self.start_upload_to_salesforce(grouped_opps, HOMEWORKS_ACCID))
        except Exception as e:
          logger.error("Error in Homeworks process.")
          logger.error(traceback.format_exc())
          logger.error(e)
    
    def run_vhi(self) -> None:
        '''
          Run VHI process
        '''
        try:
          data = read_csv(cast(str, getenv('VHI_DATA_URL')), dtype='object')
          processed_row_removed = self.remove_already_processed_row(data)
          processed_row = vhi(processed_row_removed)
          grouped_opps = to_account_and_opportunities(processed_row)
          run(self.start_upload_to_salesforce(grouped_opps, VHI_ACCID))
        except Exception as e:
          logger.error("Error in VHI process.")
          logger.error(traceback.format_exc())
          logger.error(e)

    def run(self) -> None:
        logger.debug('Start Processing Neeeco')
        self.run_neeeco()
        self.save_processed_rows()
        logger.debug('Start Processing Homeworks')
        self.run_homeworks()
        self.save_processed_rows()
        logger.debug('Start Processing VHI')
        self.run_vhi()
        self.save_processed_rows()
        logger.debug('Finished running Omnivore')


if __name__ == '__main__':
    load_dotenv()
    omni = Blueprint()
    omni.run()