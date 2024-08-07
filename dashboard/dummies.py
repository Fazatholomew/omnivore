from dashboard.models import Telemetry, HPC, Data
from datetime import datetime, timedelta
from json import load, dumps
from os import listdir


def generate_examples():
    results = {}
    for current_file in listdir("dashboard/dummies/"):
        if "json" not in current_file:
            continue
        splited = current_file.split(" ")
        if "Valley" in current_file:
            hpc_name = "Valley Home Insulation"
            data_name = splited[-1].replace(".json", "")
        else:
            hpc_name = splited[1]
            data_name = " ".join(splited[2:]).replace(".json", "")
        with open(f"dashboard/dummies/{current_file}") as current_json:
            current_data = load(current_json)
            if hpc_name not in results:
                results[hpc_name] = {}
            results[hpc_name][data_name] = current_data
    return results


def generate_dummies():
    examples = generate_examples()
    return [
        Telemetry(id="test"),
        HPC(
            name="Homeworks",
            start_time=datetime.now() - timedelta(minutes=100),
            end_time=datetime.now(),
            examples=examples["Homeworks"],
            input=1223,
            output=734,
            acc_created=300,
            acc_updated=124,
            opp_created=500,
            opp_updated=230,
            telemetry_id="test",
        ),
        HPC(
            name="Neeeco",
            start_time=datetime.now() - timedelta(minutes=20),
            end_time=datetime.now(),
            examples=examples["Neeeco"],
            input=500,
            output=233,
            acc_created=100,
            acc_updated=124,
            opp_created=123,
            opp_updated=100,
            telemetry_id="test",
        ),
        HPC(
            name="Revise",
            start_time=datetime.now() - timedelta(minutes=1),
            end_time=datetime.now(),
            examples=examples["Revise"],
            input=322,
            output=123,
            acc_created=50,
            acc_updated=20,
            opp_created=20,
            opp_updated=95,
            telemetry_id="test",
        ),
        HPC(
            name="Valley Home Insulation",
            start_time=datetime.now() - timedelta(minutes=0.1),
            end_time=datetime.now(),
            examples=examples["Valley Home Insulation"],
            input=165,
            output=90,
            acc_created=33,
            acc_updated=45,
            opp_created=10,
            opp_updated=20,
            telemetry_id="test",
        ),
        Data(
            hpc_name="Homeworks",
            created_date=datetime.now() - timedelta(days=2),
            source="Canceled & Completed Email",
            row_number=1223,
            telemetry_id="test",
        ),
        Data(
            hpc_name="Neeeco",
            created_date=datetime.now() - timedelta(days=1),
            source="HEA, CFP, Weatherization Sheet",
            row_number=500,
            telemetry_id="test",
        ),
        Data(
            hpc_name="Revise",
            created_date=datetime.now() - timedelta(days=3),
            source="10 HEA & 10 Weatherization Emails",
            row_number=322,
            telemetry_id="test",
        ),
        Data(
            hpc_name="Valley Home Insulation",
            created_date=datetime.now() - timedelta(days=10),
            source="Lead & Opp Email",
            row_number=165,
            telemetry_id="test",
        ),
        Telemetry(id="test1"),
        HPC(
            name="Homeworks",
            start_time=datetime.now() - timedelta(minutes=5),
            end_time=datetime.now(),
            examples="",
            input=1213,
            output=754,
            acc_created=300,
            acc_updated=124,
            opp_created=580,
            opp_updated=210,
            telemetry_id="test1",
        ),
        HPC(
            name="Neeeco",
            start_time=datetime.now() - timedelta(minutes=20),
            end_time=datetime.now(),
            examples="",
            input=400,
            output=133,
            acc_created=100,
            acc_updated=124,
            opp_created=23,
            opp_updated=100,
            telemetry_id="test1",
        ),
        HPC(
            name="Revise",
            start_time=datetime.now() - timedelta(minutes=13),
            end_time=datetime.now(),
            examples="",
            input=122,
            output=93,
            acc_created=50,
            acc_updated=20,
            opp_created=10,
            opp_updated=75,
            telemetry_id="test1",
        ),
        HPC(
            name="Valley Home Insulation",
            start_time=datetime.now() - timedelta(minutes=2),
            end_time=datetime.now(),
            examples="",
            input=300,
            output=100,
            acc_created=33,
            acc_updated=45,
            opp_created=50,
            opp_updated=49,
            telemetry_id="test1",
        ),
        Data(
            hpc_name="Homeworks",
            created_date=datetime.now() - timedelta(days=2),
            source="Canceled & Completed Email",
            row_number=1213,
            telemetry_id="test1",
        ),
        Data(
            hpc_name="Neeeco",
            created_date=datetime.now() - timedelta(days=1),
            source="HEA, CFP, Weatherization Sheet",
            row_number=400,
            telemetry_id="test1",
        ),
        Data(
            hpc_name="Revise",
            created_date=datetime.now() - timedelta(days=3),
            source="10 HEA & 10 Weatherization Emails",
            row_number=122,
            telemetry_id="test1",
        ),
        Data(
            hpc_name="Valley Home Insulation",
            created_date=datetime.now() - timedelta(days=10),
            source="Lead & Opp Email",
            row_number=300,
            telemetry_id="test1",
        ),
    ]
