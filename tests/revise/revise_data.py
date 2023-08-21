from pandas import read_csv
from ast import literal_eval

hea_data = read_csv("./tests/revise/Revise Input.csv", dtype="object")
wx_data = read_csv("./tests/revise/Revise Wx Input.csv", dtype="object")
output_data = read_csv("./tests/revise/Revise output.csv", dtype="object")
output_data["Health_Safety_Barrier__c"] = output_data["Health_Safety_Barrier__c"].apply(
    literal_eval
)
output_data["Do_Not_Contact__c"] = output_data["Do_Not_Contact__c"].apply(literal_eval)
