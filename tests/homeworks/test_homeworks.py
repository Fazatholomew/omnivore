from omnivore.homeworks.homeworks import homeworks, rename_and_merge
from .homeworks_data import input_new_data, input_old_data, output_data
from pandas import isna

def test_homeworks_processing_function():
    data = rename_and_merge(input_old_data, input_new_data)
    result = homeworks(data)
    for i in range(len(output_data)):
      for column in output_data.columns:
        expected_value = output_data.iloc[i][column] # type: ignore
        current_value = result.iloc[i][column] # type: ignore
        if isna(expected_value):
          assert isna(current_value), f"\nColumn = '{column}'\nHomeworks code = {current_value}\nExpected value = {expected_value}\nHomeworks ID = {output_data.iloc[i]['ID_from_HPC__c']}\n"
        else:
          assert expected_value == current_value, f"\nColumn = '{column}'\nHomeworks code = {current_value}\nExpected value = {expected_value}\nHomeworks ID = {output_data.iloc[i]['ID_from_HPC__c']}\n"
