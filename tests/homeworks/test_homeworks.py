from omnivore.homeworks.homeworks import homeworks
from .homeworks_data import input_data, output_data
from pandas import isna
def test_homeworks_processing_function():
  result = homeworks(input_data)
  for i in range(len(output_data) - 1):
    for column in output_data.columns:
      expected_value = output_data.iloc[i][column]
      current_value = result.iloc[i][column]
      if isna(expected_value):
        assert isna(current_value), f"\nColumn = '{column}'\nNeeeco code = {current_value}\nExpected value = {expected_value}\nNeeeco ID = {output_data.iloc[i]['ID_from_HPC__c']}\n"
      else:
        assert expected_value == current_value, f"\nColumn = '{column}'\nNeeeco code = {current_value}\nExpected value = {expected_value}\nNeeeco ID = {output_data.iloc[i]['ID_from_HPC__c']}\n"
