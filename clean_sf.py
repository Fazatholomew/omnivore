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
res = sf.query_all("SELECT Id FROM Opportunity")
run(start_deleting(sf.Opportunity.delete, [opp["Id"] for opp in res["records"]]))
res = sf.query_all("SELECT Id FROM Account WHERE RecordTypeId != '0123i000000q3moAAA'")
run(start_deleting(sf.Account.delete, [acc["Id"] for acc in res["records"]]))
