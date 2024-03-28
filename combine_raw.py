from pandas import read_csv, concat
from os import listdir
from hashlib import md5
from pandas.errors import ParserError

vhi = []
hwe = []
hweCompleted = []

results = [
    {
        "data": [],
        "keyword": "VHI",
        "ID": "VHI Unique Number",
    },
    {
        "data": [],
        "keyword": "Completed",
        "ID": "Operations: Unique ID",
    },
    {
        "data": [],
        "keyword": "Canceled",
        "ID": "Operations: Operations ID & Payzer Memo",
    },
]

for filename in listdir("Omnivore 2"):
    if "csv" not in filename:
        continue
    try:
        data = read_csv(f"Omnivore 2/{filename}", dtype="object")
    except ParserError:
        print(filename)
    for result in results:
        if result["keyword"] in filename:
            result["data"].append(data)

for result in results:
    combined = concat(result["data"])
    if result["keyword"] == "VHI":
        combined = combined[~combined["Opportunity Name"].isna()].copy()
        combined["VHI Unique Number"] = combined["VHI Unique Number"].fillna(
            combined["Opportunity Name"].apply(
                lambda x: md5(x.encode("utf-8")).hexdigest()
            )
        )
    dedup = combined.drop_duplicates([result["ID"]])
    dedup.to_csv(f'{result["keyword"]}.csv', index=False)
