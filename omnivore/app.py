from omnivore.cambridge.cambridge import cambridge
from omnivore.homeworks.homeworks import homeworks, rename_and_merge
from omnivore.neeeco.neeeco import neeeco
from omnivore.vhi.vhi import vhi
from omnivore.revise.revise import revise, merge_file_revise
from omnivore.utils.salesforce import SalesforceConnection, Create
from omnivore.utils.aux import (
    to_account_and_opportunities,
    to_sf_payload,
    find_cfp_campaign,
    save_output_df,
)
from omnivore.utils.types import Record_Find_Info
from omnivore.utils.constants import (
    NEEECO_ACCID,
    HEA_ID,
    CFP_OPP_ID,
    HOMEWORKS_ACCID,
    VHI_ACCID,
    REVISE_ACCID,
    CAMBRIDGE_ACCID,
    CAMBRIDGE_OPP_ID,
)
from os import getenv
from pickle import load, dump
from typing import cast
from pandas import DataFrame, read_csv
from asyncio import run, gather, get_event_loop
from simple_salesforce.exceptions import SalesforceMalformedRequest
from hashlib import md5
from dataclasses import dataclass
from datetime import datetime
from aiohttp import ClientSession
from io import StringIO
from requests import post
from datetime import datetime
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


@dataclass
class HPCDTO:
    # Class to keep track of hpc telemetry
    name: str
    start_time: datetime
    end_time: datetime = None
    examples: dict = {}
    input: int = 0
    output: int = 0
    acc_created: int = 0
    acc_updated: int = 0
    opp_created: int = 0
    opp_updated: int = 0
    telemetry_id: str


@dataclass
class DataDTO:
    # Class to keep track of data telemetry
    hpc_name: str
    created_date: datetime
    source: str
    row_number: int
    telemetry_id: str


class Blueprint:
    def __init__(self) -> None:
        self.data: dict[str, DataFrame] = {}
        self.hpcs: dict[str, HPCDTO] = {}
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
        logger.info("Fetching telemetry ID")
        respose = post(
            f'{cast(str, getenv("DASHBOARD_URL"))}/telemetry',
            json={"rahasia": getenv("RAHASIA")},
        )
        self.telemetry_id = respose["id"]
        logger.info(f"Current Telemetry ID: {self.telemetry_id}")

    async def get_data_GAS(self, url: str, hpc: str, session: ClientSession):
        try:
            async with session.post(
                url, json={"rahasia": getenv("RAHASIA")}
            ) as response:
                current_data = await response.json()

            df = read_csv(StringIO(current_data["data"]), dtype="object")
            self.data[url] = df

            current_telemetry_data = DataDTO(
                hpc,
                datetime.fromtimestamp(float(current_data["created_date"])),
                current_data["source"],
                len(df),
                self.telemetry_id,
            )

            try:
                async with session.post(
                    f'{cast(str, getenv("DASHBOARD_URL"))}/data',
                    json=current_telemetry_data.__dict__,
                ) as response:
                    response = await response.json()

            except Exception as err:
                logger.error(f"Failed to record telemetry for {url}")
                logger.error(err, exc_info=True)
        except Exception as err:
            logger.error(f"Failed to fetch {url}")
            logger.error(err, exc_info=True)

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
        result["tempId"] = result.fillna("").T.agg("".join).str.lower()
        return result[~result["tempId"].isin(self.processed_rows)].copy()

    def upload_to_salesforce(self, data: Record_Find_Info, HPC_ID):
        """
        Find and match opportunity then upload them
        """
        if len(data["opps"]) == 0:
            # No need to process if empty opp
            return
        found_records = self.sf.find_records(data)
        acc_payload = to_sf_payload(data["acc"])
        try:
            res = self.sf.sf.Account.update(found_records[0]["AccountId"], acc_payload)
            if cast(int, res) > 399:
                logger.error(res)
            logger.debug(res)
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
                    except Exception as e:
                        if getenv("ENV") == "staging":
                            logger.error("failed to update after create")
                            logger.error(acc_payload)
                            logger.error(e, exc_info=True)
                            raise e
                        logger.error(e, exc_info=True)
        except Exception as err:
            if getenv("ENV") == "staging":
                logger.error("failed to update")
                logger.error(data["acc"])
                logger.error(err, exc_info=True)
                raise err
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
                            logger.debug(res)
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
                                                if cast(int, res) > 399:
                                                    logger.error(res)
                                            except Exception as e:
                                                if getenv("ENV") == "staging":
                                                    logger.error(
                                                        "failed to update after create"
                                                    )
                                                    logger.error(payload)
                                                    logger.error(e, exc_info=True)
                                                    raise e
                                                logger.error(e, exc_info=True)
                                except Exception as e:
                                    if getenv("ENV") == "staging":
                                        logger.error(payload)
                                        logger.error(e, exc_info=True)
                                        raise e
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
                                        if cast(int, res) > 399:
                                            logger.error(res)
                                    except Exception as e:
                                        if getenv("ENV") == "staging":
                                            logger.error(
                                                "failed to update after create"
                                            )
                                            logger.error(payload)
                                            logger.error(e, exc_info=True)
                                            raise e
                                        logger.error(e, exc_info=True)
                            else:
                                logger.error(err, exc_info=True)
                        except Exception as err:
                            if getenv("ENV") == "staging":
                                logger.error("failed to update")
                                logger.error(payload)
                                logger.error(err, exc_info=True)
                                raise err
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
                            # Reporting
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
                                    if cast(int, res) > 399:
                                        logger.error(res)
                                except Exception as e:
                                    if getenv("ENV") == "staging":
                                        logger.error("failed to update after create")
                                        logger.error(payload)
                                        logger.error(e, exc_info=True)
                                        raise e
                                    logger.error(e, exc_info=True)

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
                                if cast(int, res) > 399:
                                    logger.error(res)
                            except Exception as e:
                                if getenv("ENV") == "staging":
                                    logger.error(
                                        "failed to create after cancelation reason"
                                    )
                                    logger.error(payload)
                                    logger.error(e, exc_info=True)
                                    raise e
                                logger.error(e, exc_info=True)
                        if getenv("ENV") == "staging":
                            logger.error(payload)
                            logger.error("failed to update after create")
                            logger.error(err, exc_info=True)
                            raise err
                        logger.error(err, exc_info=True)

                    except Exception as e:
                        logger.error("error creating")
                        if getenv("ENV") == "staging":
                            logger.error(payload)
                            logger.error("failed to create")
                            logger.error(e)
                            raise e
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
            self.hpcs[NEEECO_ACCID] = HPCDTO(
                NEEECO_ACCID,
                datetime.now(),
                telemetry_id=self.telemetry_id,
                input=len(raw_data),
            )
            raw_data = raw_data[~raw_data["ID"].isna()]
            sample_input = raw_data.sample(10)
            sample_wx = wx_data[
                wx_data["HEA - Last, First, Address"].isin(sample_input["Related to"])
            ]
            save_output_df(sample_input, "Neeeco Input", "json")
            save_output_df(sample_wx, "Neeeco Wx Input", "json")
            processed_row_removed = self.remove_already_processed_row(raw_data)
            processed_row = neeeco(processed_row_removed, wx_data)
            processed_row = processed_row[~processed_row["ID_from_HPC__c"].isna()]
            processed_row = processed_row.drop_duplicates(["ID_from_HPC__c"])
            self.hpcs[NEEECO_ACCID].output = len(processed_row)
            sample_output = processed_row[
                processed_row["ID_from_HPC__c"].isin(sample_input["ID"])
            ]
            save_output_df(
                sample_output,
                "Neeeco output",
                "json",
            )
            self.hpcs[NEEECO_ACCID].examples = {
                "Neeeco Input": sample_input,
                "Neeeco Wx Input": sample_wx,
                "Neeeco output": sample_output,
            }
            grouped_opps = to_account_and_opportunities(processed_row)
            run(self.start_upload_to_salesforce(grouped_opps, NEEECO_ACCID))
            self.hpcs[NEEECO_ACCID].end_time = datetime.now()

            try:
                post(
                    f'{cast(str, getenv("DASHBOARD_URL"))}/telemetry',
                    json=self.hpcs[NEEECO_ACCID].__dict__,
                )
            except Exception as e:
                logger.error("Failed to record telemetry for Neeeco")
                logger.error(e, exc_info=True)

        except Exception as e:
            logger.error("Error in Neeeco process.")
            logger.error(e, exc_info=True)

    def run_homeworks(self) -> None:
        """
        Run Homeworks process
        """
        try:
            new_data = read_csv(cast(str, getenv("HOMEWORKS_DATA_URL")), dtype="object")
            old_data = read_csv(
                cast(str, getenv("HOMEWORKS_COMPLETED_DATA_URL")), dtype="object"
            )
            data_sample_completed = old_data.sample(10)
            data_sample = new_data.sample(10)
            save_output_df(data_sample_completed, "Homeworks Completed Input", "json")
            save_output_df(data_sample, "Homeworks Input Canceled", "json")
            homeworks_output = rename_and_merge(old_data, new_data)
            processed_row_removed = self.remove_already_processed_row(homeworks_output)
            processed_row = homeworks(processed_row_removed)
            processed_row = processed_row.drop_duplicates(["ID_from_HPC__c"])
            save_output_df(
                processed_row[
                    processed_row["ID_from_HPC__c"].isin(
                        data_sample_completed["Operations: Operations ID & Payzer Memo"]
                    )
                ],
                "Homeworks Output",
                "json",
            )
            processed_row = processed_row[~processed_row["ID_from_HPC__c"].isna()]
            grouped_opps = to_account_and_opportunities(processed_row)

            run(self.start_upload_to_salesforce(grouped_opps, HOMEWORKS_ACCID))
            # save_output_df(processed_row, "Homeworks")
        except Exception as e:
            logger.error("Error in Homeworks process.")
            logger.error(e, exc_info=True)

    def run_vhi(self) -> None:
        """
        Run VHI process
        """
        try:
            data = read_csv(cast(str, getenv("VHI_DATA_URL")), dtype="object")
            processed_row_removed = self.remove_already_processed_row(data)
            processed_row_removed["VHI Unique Number"] = processed_row_removed[
                "VHI Unique Number"
            ].fillna(
                processed_row_removed["Opportunity Name"].apply(
                    lambda x: md5(x.encode("utf-8")).hexdigest()
                )
            )
            data_sample = processed_row_removed.sample(10)
            save_output_df(data_sample, "Valley Home Instulation Input", "json")
            processed_row = vhi(processed_row_removed)
            processed_row = processed_row.drop_duplicates(["ID_from_HPC__c"])
            save_output_df(
                processed_row[
                    processed_row["ID_from_HPC__c"].isin(
                        data_sample["VHI Unique Number"]
                    )
                ],
                "Valley Home Instulation Output",
                "json",
            )
            processed_row = processed_row[~processed_row["ID_from_HPC__c"].isna()]
            grouped_opps = to_account_and_opportunities(processed_row)
            run(self.start_upload_to_salesforce(grouped_opps, VHI_ACCID))
        except Exception as e:
            logger.error("Error in VHI process.")
            logger.error(e, exc_info=True)

    def run_revise(self) -> None:
        """
        Run Revise process
        """
        try:
            hea_data = read_csv(cast(str, getenv("REVISE_DATA_URL")), dtype="object")
            wx_data = read_csv(cast(str, getenv("REVISE_WX_DATA_URL")), dtype="object")
            hea_data = hea_data[
                hea_data["Company / Account"] != "Company / Account"
            ].copy()
            wx_data = wx_data[wx_data["Account Name"] != "Account Name"].copy()
            data_sample = hea_data.sample(10)
            wx_data_sample = wx_data[
                wx_data["Account Name"].isin(data_sample["Company / Account"])
            ]
            save_output_df(data_sample, "Revise Input", "json")
            save_output_df(wx_data_sample, "Revise Wx Input", "json")
            data = merge_file_revise(hea_data, wx_data)
            processed_row_removed = self.remove_already_processed_row(data)
            processed_row = revise(processed_row_removed)
            processed_row = processed_row[~processed_row["ID_from_HPC__c"].isna()]
            processed_row = processed_row.drop_duplicates(["ID_from_HPC__c"])
            save_output_df(
                processed_row[
                    processed_row["ID_from_HPC__c"].isin(
                        data_sample["Company / Account"]
                    )
                ],
                "Revise Output",
                "json",
            )
            grouped_opps = to_account_and_opportunities(processed_row)
            run(self.start_upload_to_salesforce(grouped_opps, REVISE_ACCID))
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
        logger.info("Load Database from SF")
        self.sf.get_salesforce_table()
        logger.info("Finsihed loading Database from SF")
        logger.info("Start Processing Omnivore")
        logger.info("Start Processing Neeeco")
        self.run_neeeco()
        self.save_processed_rows()
        self.sf.get_salesforce_table()
        logger.info("Start Processing Homeworks")
        self.run_homeworks()
        self.save_processed_rows()
        self.sf.get_salesforce_table()
        logger.info("Start Processing VHI")
        self.run_vhi()
        self.save_processed_rows()
        self.sf.get_salesforce_table()
        logger.info("Start Processing Revise")
        self.run_revise()
        self.save_processed_rows()
        self.sf.get_salesforce_table(True)
        logger.info("Start Processing Cambridge")
        self.run_cambridge()
        self.save_processed_rows()
        logger.info("Finished running Omnivore")
