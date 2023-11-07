from simple_salesforce import Salesforce
from os import getenv
from asyncio import get_event_loop, gather, run


async def start_deleting(fun, ids):
    loop = get_event_loop()
    await gather(*[loop.run_in_executor(None, fun, current_id) for current_id in ids])


sf = Salesforce(
    username=getenv("EMAIL"),
    consumer_key=getenv(  # type:ignore
        "CONSUMER_KEY"
    ),
    privatekey_file=getenv("PRIVATEKEY_FILE"),
    domain="test",
)
print(sf.bulk_url)

samples = '''"CloseDate","Name","StageName","ID_from_HPC__c"
"2021-01-01T12:00:00.000-07:00","Jimmy","Not Yet Scheduled","testest"
"2021-01-01T12:00:00.000-07:00","Jimmy1","Not Yet Scheduled","testest"'''

account_samples = '''"LastName","Phone","PersonEmail","RecordTypeId"
"jimmy","9836647281","jimm@gmail.com","0123i000000YGesAAG"
"JImmy","9836647281","jimm@gmail.com","0123i000000YGesAAG"'''

with open("account_sample.csv", "w") as f:
    f.write(account_samples)


results = sf.bulk2.Account.insert(
    "./account_sample.csv", batch_size=10000, concurrency=10
)
# [{"numberRecordsFailed": 123, "numberRecordsProcessed": 2000, "numberRecordsTotal": 2000, "job_id": "Job-1"}, ...]
for result in results:
    job_id = result["job_id"]
    # also available: get_unprocessed_records, get_successful_records
    print("result")
    print(result)
    print("failed")
    # Opportunity Error
    #  "sf__Id","sf__Error","CloseDate","Name","StageName","ID_from_HPC__c"
    # "","DUPLICATE_VALUE:duplicate value found: ID_from_HPC__c duplicates value on record with id: 006Ov000004iYzqIAE:--","2021-01-01","Jimmy1","Not Yet Scheduled","testest"
    print(sf.bulk2.Account.get_failed_records(job_id))
    print("unprocessed")
    print(sf.bulk2.Account.get_unprocessed_records(job_id))
    print("success")
    print(sf.bulk2.Account.get_successful_records(job_id))
