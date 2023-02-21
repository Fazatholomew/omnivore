from omnivore.neeeco.neeeco import neeeco
from .neeeco_data import input_wx_data, input_data, output_data
from pandas import isna
def test_neeeco_processing_function():
  result = neeeco(input_data, input_wx_data)
  for i in range(len(output_data)):
    for column in output_data.columns:
      expected_value = output_data.iloc[i][column]
      current_value = result.iloc[i][column]
      if isna(expected_value):
        assert isna(current_value), f"\nColumn =\n'{column}'\nNeeeco code =\n{current_value}\nExpected value =\n{expected_value}\nNeeeco ID = {output_data.iloc[i]['ID_from_HPC__c']}\n"
      else:
        assert expected_value == current_value, f"\nColumn = '{column}'\nNeeeco code =\n{current_value}\nExpected value =\n{expected_value}\nNeeeco ID = {output_data.iloc[i]['ID_from_HPC__c']}\n"
