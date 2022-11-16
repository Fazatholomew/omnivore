from omnivore.neeeco.neeeco import neeeco
from .neeeco_data import input_wx_data, input_data, output_data
def test_neeeco_processing_function():
  result = neeeco(input_data, input_wx_data)
  for i in range(len(output_data) - 1):
    for column in output_data.columns:
      expected_value = output_data.iloc[i][column]
      current_value = result.iloc[i][column]
      assert expected_value == current_value, f"\nColumn = '{column}'\nNeeeco code = {current_value}\nExpected value = {expected_value}\nNeeeco ID = {output_data.iloc[i]['ID_from_HPC__c']}\n"
