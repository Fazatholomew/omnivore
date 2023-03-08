from omnivore.vhi.vhi import vhi
from .vhi_data import input_data, output_data
from pandas import isna

def test_vhi_processing_function():
    result = vhi(input_data)
    for i in range(len(result)):
      for column in output_data.columns:
        expected_value = output_data.iloc[i][column] # type: ignore
        current_value = result.iloc[i][column] # type: ignore
        if isna(expected_value):
          assert isna(current_value), f"\nColumn = '{column}'\nVHI code = {current_value}\nExpected value = {expected_value}\nVHI ID = {output_data.iloc[i]['ID_from_HPC__c']}\n"
        else:
          assert expected_value == current_value, f"\nColumn = '{column}'\nVHI code = {current_value}\nExpected value = {expected_value}\nVHI ID = {output_data.iloc[i]['ID_from_HPC__c']}\n"
