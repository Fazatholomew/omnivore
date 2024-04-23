from omnivore.cambridge.cambridge import cambridge
from omnivore.homeworks.homeworks import homeworks, rename_and_merge
from omnivore.neeeco.neeeco import neeeco, merge_neeeco
from omnivore.vhi.vhi import vhi
from omnivore.revise.revise import revise, merge_file_revise
from omnivore.utils.salesforce import SalesforceConnection
from omnivore.utils.aux import (
    to_account_and_opportunities,
    to_sf_payload,
    find_cfp_campaign,
    save_output_df,
)
from omnivore.utils.types import Record_Find_Info, Create
from omnivore.utils.constants import (
    NEEECO_ACCID,
    HEA_ID,
    CFP_OPP_ID,
    HOMEWORKS_ACCID,
    VHI_ACCID,
    REVISE_ACCID,
    CAMBRIDGE_ACCID,
    CAMBRIDGE_OPP_ID,
    HPC_DATA_URLS,
    PERSON_ACCOUNT_ID,
    HPCIDTOHPCNAME
)
from omnivore.utils.database import init_db

from dashboard.models import Telemetry, HPC, Data

from os import getenv
from pickle import load, dump
from typing import cast
from pandas import DataFrame, read_csv
from asyncio import run, gather, get_event_loop
from simple_salesforce.exceptions import SalesforceMalformedRequest
from hashlib import md5
from datetime import datetime
from aiohttp import ClientSession
from io import StringIO
from concurrent.futures import ThreadPoolExecutor

import logging

logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    filename="omnivore.log",
    filemode="a",
)

logging.FileHandler("omnivore.log")


# Create a logger object
logger = logging.getLogger(__name__)

class Blueprint:
    def __init__(self) -> None:
        self.db = init_db()
        self.data: dict[str, DataFrame] = {}
        self.hpcs: dict[str, HPC] = {}
        if getenv("EMAIL") or getenv("ENV") == "test":
            self.sf = SalesforceConnection(
                username=getenv("EMAIL"),
                consumer_key=getenv(  # type:ignore
                    "CONSUMER_KEY"
                ),
                privatekey_file=getenv("PRIVATEKEY_FILE"),
            )  # type:ignore
        self.load_processed_rows()
        logger.info("Connection to Salesforce is successful")
        try:
          logger.info("Fetching telemetry ID")
          current_telemetry = Telemetry()
          self.db.add(current_telemetry)
          self.db.commit()
          self.telemetry_id = current_telemetry.id
          logger.info(f"Current Telemetry ID: {self.telemetry_id}")
        except Exception as err:
          logger.error("Failed getting Telemetry ID. Omnivore continues without sending Telemetry.")
          logger.error(err, exc_info=True)

    async def async_init(self):
        # Executor for running synchronous tasks in a separate thread
        executor = ThreadPoolExecutor(max_workers=1)

        # Asynchronous tasks
        async with ClientSession() as session:
            fetch_tasks = [
                self.get_data_GAS(current_url, session) for current_url in HPC_DATA_URLS
            ]

            # Synchronous task packaged for async execution
            sync_task = get_event_loop().run_in_executor(
                executor, self.sf.get_salesforce_table
            )

            await gather(*(fetch_tasks + [sync_task]))

            # Gather all tasks (both sync and async)
            # completed, _ = await wait(
            #     fetch_tasks + [sync_task], return_when=ALL_COMPLETED
            # )

    async def get_data_GAS(self, url: str, session: ClientSession):
        try:
            logger.info(f"Fetching {url}")
            async with session.post(
                getenv(url), json={"rahasia": getenv("RAHASIA")}
            ) as response:
                current_data = await response.json()

            df = read_csv(StringIO(current_data["data"]), dtype="object")
            self.data[url] = df

            if (self.telemetry_id):
              current_telemetry_data = Data(
                  hpc_name=current_data["hpc"],
                  created_date=datetime.fromtimestamp(int(current_data["created_date"])),
                  source=current_data["source"],
                  row_number=len(df),
                  telemetry_id=self.telemetry_id,
              )
              try:
                  self.db.add(current_telemetry_data)
                  self.db.commit()
              except Exception as err:
                  logger.error(f"Failed to record data telemetry for {url}")
                  logger.error(err, exc_info=True)

            logger.info(f"Finished fetching {url}")

        except Exception as err:
            logger.error(f"Failed to fetch {url}")
            logger.error(err, exc_info=True)

    def post_hpc_telemetry(self, hpc_id: str):
        self.db.add(self.hpcs[hpc_id])
        self.db.commit()

    def load_processed_rows(self, fileName="./processed_row") -> None:
        """
        Load already processed rows from pickled file
        """
        try:
            with open(fileName, "rb") as file_blob:
                self.processed_rows = cast(set[str], load(file_blob))
        except FileNotFoundError:
            self.processed_rows = set()

    def save_processed_rows(self, fileName="./processed_row") -> None:
        """
        Save processed rows into pickled file
        """
        with open(fileName, "wb") as file_blob:
            dump(self.processed_rows, file_blob)

    def remove_already_processed_row(self, data: DataFrame) -> DataFrame:
        """
        Remove rows that already processed and no changes occures
        Add temp id for processed row detection
        """
        result = data.copy()
        if 'tempId' not in data.columns:
          result["tempId"] = result.fillna("").T.agg("".join).str.lower()
        return result[~result["tempId"].isin(self.processed_rows)].copy()
    
    def generate_tempId(self, data: DataFrame) -> DataFrame:
        result = data.copy()
        result["tempId"] = result.fillna("").T.agg("".join).str.lower()
        return result

    def upload_to_salesforce(self, data: Record_Find_Info, HPC_ID):
        """
        Find and match opportunity then upload them
        """
        if len(data["opps"]) == 0:
            # No need to process if empty opp
            return
        found_records = self.sf.find_records(data)
        acc_payload = to_sf_payload(data["acc"])
        if "AccountId" not in found_records[0] or len(found_records[0]) < 3:
            # Account not found create a new account
            acc_payload["RecordTypeId"] = PERSON_ACCOUNT_ID
            # Final check on required field of lastname
            if "LastName" not in acc_payload:
                if "FirstName" in acc_payload:
                    acc_payload["LastName"] = acc_payload["FirstName"]
                else:
                    acc_payload["LastName"] = "Unknown"
            try:
                res: Create = cast(
                    Create, self.sf.sf.Account.create(to_sf_payload(acc_payload))
                )
                if res["success"]:
                    for opp in found_records:
                        opp["AccountId"] = res["id"]
                    self.hpcs[HPC_ID].acc_created += 1
                if len(res["errors"]) > 0:
                    for error in res["errors"]:
                        logger.error(error)
                        return
            except SalesforceMalformedRequest as e:
                if e.content[0]["errorCode"] == "DUPLICATES_DETECTED":
                    current_id = e.content[0]["duplicateResult"]["matchResults"][0][
                        "matchRecords"
                    ][0]["record"]["Id"]
                    for opp in found_records:
                        opp["AccountId"] = current_id
                else:
                    logger.error(f"Failed to create account {acc_payload}")
                    logger.error(e, exc_info=True)
        else:
            try:
                res = self.sf.sf.Account.update(
                    found_records[0]["AccountId"], acc_payload
                )
                if cast(int, res) > 399:
                    logger.error(res)
                else:
                  self.hpcs[HPC_ID].acc_updated += 1
            except SalesforceMalformedRequest as err:
                if err.content[0]["errorCode"] == "DUPLICATE_VALUE":
                    maybe_id = err.content[0]["message"].split("id: ")
                    if len(maybe_id) == 2:
                        try:
                            res = self.sf.sf.Account.update(
                                maybe_id[1], acc_payload
                            )  # type:ignore
                            if cast(int, res) > 399:
                                logger.error(res)
                            else:
                                self.hpcs[HPC_ID].acc_updated += 1
                        except Exception as e:
                            logger.error(f"Failed to update account {found_records[0]["AccountId"]}")
                            logger.error(e, exc_info=True)
            except Exception as err:
                logger.error(f"Failed to update account {found_records[0]["AccountId"]}")
                logger.error(err, exc_info=True)
        for opp in found_records:
            try:
                # Remove and keep tempId for processed row
                processed_row_id = (
                    opp.pop("tempId") if "tempId" in opp else opp["ID_from_HPC__c"]
                )
                if "Don_t_Omnivore__c" in opp:
                    # Don't omnivore is flagged
                    if opp["Don_t_Omnivore__c"]:
                        self.processed_rows.add(processed_row_id)
                        continue
                opp["HPC__c"] = HPC_ID
                if HPC_ID == CAMBRIDGE_ACCID:
                    opp["RecordTypeId"] = CAMBRIDGE_OPP_ID
                else:
                    opp["CampaignId"] = find_cfp_campaign(opp)
                    opp["RecordTypeId"] = CFP_OPP_ID if opp["CampaignId"] else HEA_ID
                if "Id" in opp:
                    if len(opp["Id"]) > 3:
                        payload = to_sf_payload(opp, "Opportunity")
                        current_id = payload.pop("Id")
                        try:
                            res = self.sf.sf.Opportunity.update(
                                current_id, payload
                            )  # type:ignore
                            if cast(int, res) > 200 and cast(int, res) < 300:
                                self.processed_rows.add(processed_row_id)
                                # Reporting
                            if cast(int, res) > 399:
                                logger.error(res)
                            else:
                                self.hpcs[HPC_ID].opp_updated += 1
                        except SalesforceMalformedRequest as err:
                            if (
                                err.content[0]["errorCode"]
                                == "FIELD_CUSTOM_VALIDATION_EXCEPTION"
                                and "cancelation" in err.content[0]["message"]
                            ):
                                # Canceled stage needs a cancelation reason
                                try:
                                    payload["Cancelation_Reason_s__c"] = (
                                        "No Reason"  # Default reason
                                    )
                                    res = self.sf.sf.Opportunity.create(
                                        payload
                                    )  # type:ignore
                                    if cast(int, res) > 200 and cast(int, res) < 300:
                                        self.processed_rows.add(processed_row_id)
                                        self.hpcs[HPC_ID].opp_updated += 1
                                    if cast(int, res) > 399:
                                        logger.error(res)
                                except SalesforceMalformedRequest as err:
                                    if err.content[0]["errorCode"] == "DUPLICATE_VALUE":
                                        maybe_id = err.content[0]["message"].split(
                                            "id: "
                                        )
                                        if len(maybe_id) == 2:
                                            try:
                                                res = self.sf.sf.Opportunity.update(
                                                    maybe_id[1], payload
                                                )  # type:ignore
                                                if (
                                                    cast(int, res) > 200
                                                    and cast(int, res) < 300
                                                ):
                                                    self.processed_rows.add(
                                                        processed_row_id
                                                    )
                                                    self.hpcs[HPC_ID].opp_updated += 1
                                                if cast(int, res) > 399:
                                                    logger.error(res)
                                            except Exception as e:
                                                logger.error(e, exc_info=True)
                                except Exception as e:
                                    logger.error(
                                        "failed to create after cancelation reason"
                                    )
                                    logger.error(e, exc_info=True)
                            elif err.content[0]["errorCode"] == "DUPLICATE_VALUE":
                                maybe_id = err.content[0]["message"].split("id: ")
                                if len(maybe_id) == 2:
                                    try:
                                        res = self.sf.sf.Opportunity.update(
                                            maybe_id[1], payload
                                        )  # type:ignore
                                        if (
                                            cast(int, res) > 200
                                            and cast(int, res) < 300
                                        ):
                                            self.processed_rows.add(processed_row_id)
                                            self.hpcs[HPC_ID].opp_updated += 1
                                        if cast(int, res) > 399:
                                            logger.error(res)
                                    except Exception as e:
                                        logger.error(e, exc_info=True)
                            else:
                                logger.error(err, exc_info=True)
                        except Exception as err:
                            logger.error(err, exc_info=True)
                            continue
                else:
                    payload = to_sf_payload(opp, "Opportunity")
                    try:
                        res: Create = self.sf.sf.Opportunity.create(
                            payload
                        )  # type:ignore
                        if res["success"]:
                            self.processed_rows.add(processed_row_id)
                            self.hpcs[HPC_ID].opp_created += 1
                    except SalesforceMalformedRequest as err:
                        if err.content[0]["errorCode"] == "DUPLICATE_VALUE":
                            maybe_id = err.content[0]["message"].split("id: ")
                            if len(maybe_id) == 2:
                                try:
                                    res = self.sf.sf.Opportunity.update(
                                        maybe_id[1], payload
                                    )  # type:ignore
                                    if cast(int, res) > 200 and cast(int, res) < 300:
                                        self.processed_rows.add(processed_row_id)
                                        self.hpcs[HPC_ID].opp_updated += 1
                                    if cast(int, res) > 399:
                                        logger.error(res)
                                except Exception as e:
                                    logger.error(e, exc_info=True)

                        elif (
                            err.content[0]["errorCode"]
                            == "FIELD_CUSTOM_VALIDATION_EXCEPTION"
                            and "cancelation" in err.content[0]["message"]
                        ):
                            # Canceled stage needs a cancelation reason
                            try:
                                payload["Cancelation_Reason_s__c"] = (
                                    "No Reason"  # Default reason
                                )
                                res = self.sf.sf.Opportunity.create(
                                    payload
                                )  # type:ignore
                                if cast(int, res) > 200 and cast(int, res) < 300:
                                    self.processed_rows.add(processed_row_id)
                                    self.hpcs[HPC_ID].opp_created += 1
                                if cast(int, res) > 399:
                                    logger.error(res)
                            except Exception as e:
                                logger.error(e, exc_info=True)
                        else:
                          logger.error(err, exc_info=True)
                    except Exception as e:
                        logger.error("error creating")
                        logger.error(e, exc_info=True)
            except Exception as e:
                if "ID_from_HPC__c" in opp:
                    logger.error(f"Can't process: {opp['ID_from_HPC__c']}")
                elif "tempId" in opp:
                    logger.error(f"Can't process: {opp['tempId']}")
                else:
                    logger.error(f"Can't process:\n {' '.join(list(opp.values()))}")
                logger.error(e, exc_info=True)

    async def start_upload_to_salesforce(
        self, data: list[Record_Find_Info], HPC_ID: str
    ) -> None:
        """
        Start processing and uploading each account and opps asyncronously
        """
        loop = get_event_loop()
        await gather(
            *[
                loop.run_in_executor(None, self.upload_to_salesforce, row, HPC_ID)
                for row in data
            ]
        )

    def run_neeeco(self) -> None:
        """
        Run neeeco process
        """
        try:
            raw_data = self.data["NEEECO_DATA_URL"]
            wx_data = self.data["NEEECO_WX_DATA_URL"]
            if (self.telemetry_id):
              self.hpcs[NEEECO_ACCID] = HPC(
                  name=HPCIDTOHPCNAME[NEEECO_ACCID],
                  start_time=datetime.now(),
                  telemetry_id=self.telemetry_id,
                  input=len(raw_data),
                  acc_created=0,
                  acc_updated=0,
                  opp_created=0,
                  opp_updated=0,
              )
            raw_data = raw_data[~raw_data["ID"].isna()]
            sample_input = raw_data.sample(10)

            sample_wx = wx_data[
                wx_data["HEA - Last, First, Address"].isin(sample_input["Related to"])
            ]
            merged = merge_neeeco(raw_data, wx_data)
            input_data = self.generate_tempId(merged)
            processed_row = neeeco(input_data)
            processed_row = processed_row[~processed_row["ID_from_HPC__c"].isna()]
            processed_row = processed_row.drop_duplicates(["ID_from_HPC__c"])
            sample_output = processed_row[
                processed_row["ID_from_HPC__c"].isin(sample_input["ID"])
            ].copy()
            processed_row = self.remove_already_processed_row(processed_row)
            grouped_opps = to_account_and_opportunities(processed_row)
            run(self.start_upload_to_salesforce(grouped_opps, NEEECO_ACCID))
            if (self.telemetry_id):
              self.hpcs[NEEECO_ACCID].output = len(processed_row)
              self.hpcs[NEEECO_ACCID].examples = {
                  "Neeeco Input": sample_input.to_dict('records'),
                  "Neeeco Wx Input": sample_wx.to_dict('records'),
                  "Neeeco output": sample_output.to_dict('records'),
              }
              self.hpcs[NEEECO_ACCID].end_time = datetime.now()
              self.post_hpc_telemetry(NEEECO_ACCID)
        except Exception as e:
            logger.error("Error in Neeeco process.")
            logger.error(e, exc_info=True)
        
        logger.info('Finish processing Neeeco')

    def run_homeworks(self) -> None:
        """
        Run Homeworks process
        """
        try:
            new_data = self.data["HOMEWORKS_DATA_URL"]
            old_data = self.data["HOMEWORKS_COMPLETED_DATA_URL"]
            if (self.telemetry_id):
              self.hpcs[HOMEWORKS_ACCID] = HPC(
                  name=HPCIDTOHPCNAME[HOMEWORKS_ACCID],
                  start_time=datetime.now(),
                  telemetry_id=self.telemetry_id,
                  input=0,
                  acc_created=0,
                  acc_updated=0,
                  opp_created=0,
                  opp_updated=0,
              )
            data_sample_completed = old_data.sample(10)
            data_sample = new_data.sample(10)
            homeworks_output = rename_and_merge(old_data, new_data)
            data_input_sample = homeworks_output.sample(10)
            processed_row = homeworks(homeworks_output)
            data_output_sample = processed_row[
                    processed_row["ID_from_HPC__c"].isin(
                        data_input_sample["ID_from_HPC__c"]
                    )
                ].copy()
            processed_row = processed_row.drop_duplicates(["ID_from_HPC__c"])
            processed_row = processed_row[~processed_row["ID_from_HPC__c"].isna()]
            processed_row = self.remove_already_processed_row(processed_row)
            grouped_opps = to_account_and_opportunities(processed_row)
            run(self.start_upload_to_salesforce(grouped_opps, HOMEWORKS_ACCID))
            if (self.telemetry_id):
              self.hpcs[HOMEWORKS_ACCID].input = len(homeworks_output)
              self.hpcs[HOMEWORKS_ACCID].output = len(processed_row)
              self.hpcs[HOMEWORKS_ACCID].examples = {
                  "Homeworks Canceled Input": data_sample.to_dict('records'),
                  "Homeworks Completed Input": data_sample_completed.to_dict('records'),
                  "Homeworks Merged Input": data_input_sample.to_dict('records'),
                  "Homeworks output": data_output_sample.to_dict('records'),
              }
              self.hpcs[HOMEWORKS_ACCID].end_time = datetime.now()
              self.post_hpc_telemetry(HOMEWORKS_ACCID)
            # save_output_df(processed_row, "Homeworks")
        except Exception as e:
            logger.error("Error in Homeworks process.")
            logger.error(e, exc_info=True)

    def run_vhi(self) -> None:
        """
        Run VHI process
        """
        try:
            data = self.data["VHI_DATA_URL"]
            if (self.telemetry_id):
              self.hpcs[VHI_ACCID] = HPC(
                  name=HPCIDTOHPCNAME[VHI_ACCID],
                  start_time=datetime.now(),
                  telemetry_id=self.telemetry_id,
                  input=len(data),
                  acc_created=0,
                  acc_updated=0,
                  opp_created=0,
                  opp_updated=0,
              )
            processed_row_removed = data.copy()
            processed_row_removed["VHI Unique Number"] = processed_row_removed[
                "VHI Unique Number"
            ].fillna(
                processed_row_removed["Opportunity Name"].apply(
                    lambda x: md5(x.encode("utf-8")).hexdigest()
                )
            )
            data_sample = processed_row_removed.sample(10)
            processed_row_removed = self.generate_tempId(processed_row_removed)
            processed_row = vhi(processed_row_removed)
            processed_row = processed_row.drop_duplicates(["ID_from_HPC__c"])
            output_sample = processed_row[processed_row["ID_from_HPC__c"].isin(data_sample["VHI Unique Number"])].copy()
            processed_row = processed_row[~processed_row["ID_from_HPC__c"].isna()]
            processed_row = self.remove_already_processed_row(processed_row)
            grouped_opps = to_account_and_opportunities(processed_row)
            run(self.start_upload_to_salesforce(grouped_opps, VHI_ACCID))
            if (self.telemetry_id):
              self.hpcs[VHI_ACCID].output = len(processed_row)
              self.hpcs[VHI_ACCID].examples = {
                  "Valley Home Insulation Input": data_sample.to_dict('records'),
                  "Valley Home Insulation Output": output_sample.to_dict('records'),
              }
              self.hpcs[VHI_ACCID].end_time = datetime.now()
              self.post_hpc_telemetry(VHI_ACCID)
        except Exception as e:
            logger.error("Error in VHI process.")
            logger.error(e, exc_info=True)

    def run_revise(self) -> None:
        """
        Run Revise process
        """
        try:
            hea_data = self.data["REVISE_DATA_URL"]
            wx_data = self.data["REVISE_WX_DATA_URL"]
            if (self.telemetry_id):
              self.hpcs[REVISE_ACCID] = HPC(
                  name=HPCIDTOHPCNAME[REVISE_ACCID],
                  start_time=datetime.now(),
                  telemetry_id=self.telemetry_id,
                  input=len(hea_data),
                  acc_created=0,
                  acc_updated=0,
                  opp_created=0,
                  opp_updated=0,
              )
            hea_data = hea_data[
                hea_data["Company / Account"] != "Company / Account"
            ].copy()
            wx_data = wx_data[wx_data["Account Name"] != "Account Name"].copy()
            data_sample = hea_data.sample(10)
            wx_data_sample = wx_data[
                wx_data["Account Name"].isin(data_sample["Company / Account"])
            ].copy()
            data = merge_file_revise(hea_data, wx_data)
            merged_data_sample = data[data['ID_from_HPC__c'].isin(data_sample['Company / Account'])].copy()
            data = self.generate_tempId(data)
            processed_row = revise(data)
            output_data_sample = processed_row[processed_row["ID_from_HPC__c"].isin(data_sample["Company / Account"])].copy()
            processed_row = processed_row[~processed_row["ID_from_HPC__c"].isna()]
            processed_row = processed_row.drop_duplicates(["ID_from_HPC__c"])
            processed_row = self.remove_already_processed_row(processed_row)
            grouped_opps = to_account_and_opportunities(processed_row)
            run(self.start_upload_to_salesforce(grouped_opps, REVISE_ACCID))
            if (self.telemetry_id):
              self.hpcs[REVISE_ACCID].output = len(processed_row)
              self.hpcs[REVISE_ACCID].examples = {
                  "Revise Input": data_sample.to_dict('records'),
                  "Revise Wx Input": wx_data_sample.to_dict('records'),
                  "Revise Merged Input": merged_data_sample.to_dict('records'),
                  "Revise Output": output_data_sample.to_dict('records'),
              }
              self.hpcs[REVISE_ACCID].end_time = datetime.now()
              self.post_hpc_telemetry(REVISE_ACCID)
        except Exception as e:
            logger.error("Error in Revise process.")
            logger.error(e, exc_info=True)

    def run_cambridge(self) -> None:
        """
        Run Cambridge process
        """
        try:
            consultation_data = read_csv(
                cast(str, getenv("CAMBRIDGE_DATA_URL")), dtype="object"
            )
            quote_data = read_csv(
                cast(str, getenv("CAMBIRDGE_QUOTE_DATA_URL")), dtype="object"
            )
            new_ecology_data = read_csv(
                cast(str, getenv("CAMBIRDGE_NEW_ECOLOGY_DATA_URL")),
                dtype="object",
                encoding="Windows-1252",
            )

            consultation_data = consultation_data[
                ~consultation_data["Date of Communication"].isna()
            ]
            quote_data = quote_data[~quote_data["HP Quote: Created Date"].isna()]
            processed_consultation_removed = self.remove_already_processed_row(
                consultation_data
            )
            processed_quote_removed = self.remove_already_processed_row(quote_data)
            processed_new_ecology_removed = self.remove_already_processed_row(
                new_ecology_data
            )
            processed_row = cambridge(
                processed_consultation_removed,
                processed_quote_removed,
                processed_new_ecology_removed,
            )
            # save_output_df(processed_row, "Cambridge")
            grouped_opps = to_account_and_opportunities(processed_row)
            run(self.start_upload_to_salesforce(grouped_opps, CAMBRIDGE_ACCID))
        except Exception as e:
            logger.error("Error in Cambridge process.")
            logger.error(e, exc_info=True)

    def run(self) -> None:
        logger.info("Running on ENV = %s", getenv("ENV"))
        run(self.async_init())
        logger.info("Start Processing Omnivore")
        logger.info("Start Processing Neeeco")
        self.run_neeeco()
        self.save_processed_rows()
        logger.info("Start Processing Homeworks")
        self.run_homeworks()
        self.save_processed_rows()
        logger.info("Start Processing VHI")
        self.run_vhi()
        self.save_processed_rows()
        logger.info("Start Processing Revise")
        self.run_revise()
        self.save_processed_rows()
        self.sf.get_salesforce_table(True)
        logger.info("Start Processing Cambridge")
        self.run_cambridge()
        self.save_processed_rows()
        logger.info("Finished running Omnivore")
