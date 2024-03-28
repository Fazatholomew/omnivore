# type: ignore
from pickle import load


def load_dummy_query_all(query):
    if len(query.split("FROM Account")) == 2:
        with open("./tests/utils/account", "rb") as f:
            return load(f)

    if len(query.split("FROM Opportunity")) == 2:
        with open("./tests/utils/opportunity", "rb") as f:
            return load(f)


if __name__ == "__main__":
    opps = {}
    accs = {}
    accounts = load_dummy_query_all("dada FROM Account fsfsf")["records"]
    for acc in accounts:
        accs[acc["Id"]] = acc
    for opp in load_dummy_query_all("dada FROM Opportunity fsfsf")["records"]:
        if opp["AccountId"] not in opps:
            opps[opp["AccountId"]] = []
        opps[opp["AccountId"]].append(opp["Id"])
        print(
            f"Id = {opp['Id']} Email = {accs[opp['AccountId']]['PersonEmail'] if opp['AccountId'] in accs else ''} Phone = {accs[opp['AccountId']]['Phone'] if opp['AccountId'] in accs else ''} Account ID = {opp['AccountId']} HPC ID = {opp['ID_from_HPC__c']}"
        )
    # print(accounts)
