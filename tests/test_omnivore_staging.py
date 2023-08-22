import pytest
from unittest.mock import patch, mock_open
from omnivore import app
from omnivore.utils.constants import HEA_ID
from .neeeco.neeeco_data import output_data
from .homeworks.homeworks_data import output_data as output_data_homeworks
from .vhi.vhi_data import output_data as output_data_vhi
from .revise.revise_data import output_data as output_data_revise
from datetime import datetime
from omnivore.utils.aux import find_cfp_campaign


@pytest.fixture
def init_app(monkeypatch):
    """
    Initialize app as if the script is executed. Still use mock for reading and
    writing
    """
    with patch("omnivore.app.open", mock_open()):
        with patch("omnivore.app.load") as mock_load:
            with patch("omnivore.app.dump"):
                with patch("omnivore.app.find_cfp_campaign") as mock_find_cfp:
                    with monkeypatch.context() as m:
                        mock_find_cfp.return_value = ""
                        m.setattr(app, "NEEECO_ACCID", "0017600000VQCaXAAX")
                        m.setattr(app, "HOMEWORKS_ACCID", "0017600000VQCacAAH")
                        m.setattr(app, "VHI_ACCID", "0017600000Vb87cAAB")
                        m.setattr(app, "REVISE_ACCID", "0017600000bL2FtAAK")
                        now = datetime.now()
                        mock_load.return_value = (
                            set()
                        )  # Empty processed row so in the beginning everything is processed
                        omni = app.Blueprint()
                        yield omni
                        # Delete created records
                        opps = omni.sf.sf.query_all(
                            f"SELECT Id FROM Opportunity WHERE CreatedDate > {now.astimezone().strftime('%Y-%m-%dT%H:%M:%S.000%z')}"
                        )
                        accs = omni.sf.sf.query_all(
                            f"SELECT Id FROM Account WHERE CreatedDate > {now.astimezone().strftime('%Y-%m-%dT%H:%M:%S.000%z')}"
                        )
                        for opp in opps["records"]:
                            omni.sf.sf.Opportunity.delete(opp["Id"])  # type:ignore
                        for acc in accs["records"]:
                            omni.sf.sf.Account.delete(acc["Id"])  # type:ignore


@pytest.mark.staging
def test_connecting_to_salesforce(init_app):
    """
    Test integration with salesforce by creating and querying record.
    Assume empty sandbox
    """
    # creating Account
    res = init_app.sf.sf.Account.create({"LastName": "test test"})
    assert res["success"]
    # creating opp
    res_opp = init_app.sf.sf.Opportunity.create(
        {
            "CloseDate": datetime.now()
            .astimezone()
            .strftime("%Y-%m-%dT%H:%M:%S.000%z"),
            "RecordTypeId": HEA_ID,
            "AccountId": res["id"],
            "Name": "Test opportunity",
            "StageName": "Scheduled",
        }
    )
    assert res_opp["success"]
    # querying
    res_query = init_app.sf.sf.query_all(
        f"SELECT Id, AccountId from Opportunity WHERE RecordTypeId = '{HEA_ID}' AND Id = '{res_opp['id']}'"
    )
    assert res_query["records"][0]["Id"] == res_opp["id"]
    assert res_query["records"][0]["AccountId"] == res["id"]


@pytest.mark.staging
def test_duplicates_account(init_app: app.Blueprint):
    """
    Testing handling making duplicate account
    """
    # Create account
    created_account = init_app.sf.sf.Account.create(
        {"LastName": "Jimmy", "PersonEmail": "test@gmail.com"}
    )  # type:ignore
    found_opp = init_app.sf.find_records(
        {
            "acc": {"LastName": "Jimmy", "PersonEmail": "test@gmail.com"},
            "opps": [{"Name": "Test Test"}],
        }
    )
    assert len(found_opp) == 1
    assert found_opp[0]["AccountId"] == created_account["id"]


@pytest.mark.staging
def test_app_using_neeeco(init_app):
    """
    Testing whole app functionility including:
    - Connecting to SF and query current Database
    - Fetch Data from GAS
    - Process data for specific HPC (Neeeco)
    - Upload the data to SF
    """
    init_app.run_neeeco()
    unique_ids = output_data["ID_from_HPC__c"].unique().tolist()
    joined = "', '".join(unique_ids)
    res = init_app.sf.sf.query_all(
        f"SELECT ID_from_HPC__c from Opportunity WHERE ID_from_HPC__c IN ('{joined}')"
    )
    # assert len(res['records']) == len(unique_ids)
    ids = [current_opp["ID_from_HPC__c"] for current_opp in res["records"]]
    for current_id in unique_ids:
        assert current_id in ids


@pytest.mark.staging
def test_app_using_homeworks(init_app):
    """
    Testing whole app functionility including:
    - Connecting to SF and query current Database
    - Fetch Data from GAS
    - Process data for specific HPC (Homeworks)
    - Upload the data to SF
    """
    init_app.run_homeworks()
    unique_ids = output_data_homeworks["ID_from_HPC__c"].unique().tolist()
    joined = "', '".join(unique_ids)
    res = init_app.sf.sf.query_all(
        f"SELECT ID_from_HPC__c from Opportunity WHERE ID_from_HPC__c IN ('{joined}')"
    )
    ids = [current_opp["ID_from_HPC__c"] for current_opp in res["records"]]
    for current_id in unique_ids:
        assert current_id in ids
    assert len(res["records"]) == len(unique_ids)


@pytest.mark.staging
def test_app_using_vhi(init_app):
    """
    Testing whole app functionility including:
    - Connecting to SF and query current Database
    - Fetch Data from GAS
    - Process data for specific HPC (VHI)
    - Upload the data to SF
    """
    init_app.run_vhi()
    unique_ids = output_data_vhi["ID_from_HPC__c"].unique().tolist()
    joined = "', '".join(unique_ids)
    res = init_app.sf.sf.query_all(
        f"SELECT ID_from_HPC__c from Opportunity WHERE ID_from_HPC__c IN ('{joined}')"
    )
    ids = [current_opp["ID_from_HPC__c"] for current_opp in res["records"]]
    for current_id in unique_ids:
        assert current_id in ids
    assert len(res["records"]) == len(unique_ids)


@pytest.mark.staging
def test_app_using_revise(init_app):
    """
    Testing whole app functionility including:
    - Connecting to SF and query current Database
    - Fetch Data from GAS
    - Process data for specific HPC (Revise)
    - Upload the data to SF
    """
    init_app.run_revise()
    unique_ids = output_data_revise["ID_from_HPC__c"].unique().tolist()
    joined = "', '".join(unique_ids)
    res = init_app.sf.sf.query_all(
        f"SELECT ID_from_HPC__c from Opportunity WHERE ID_from_HPC__c IN ('{joined}')"
    )
    ids = [current_opp["ID_from_HPC__c"] for current_opp in res["records"]]
    for current_id in unique_ids:
        assert current_id in ids
    assert len(res["records"]) == len(unique_ids)
