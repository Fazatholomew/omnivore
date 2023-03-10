from omnivore.homeworks.homeworks import homeworks, rename_and_merge, combine_hs
from .homeworks_data import input_new_data, input_old_data, output_data
from pandas import isna, DataFrame

def test_homeworks_hsMapper():
  data = DataFrame([{
    "Knob and Tube": '1',
    "Asbestos": '0',	
    "Vermiculite": '1',
    "Mold": '1'
  },{
    "Knob and Tube": '1',
    "Asbestos": '0',	
    "Vermiculite": '0',
    "Mold": '1'
  },{
    "Knob and Tube": '0',
    "Asbestos": '0',	
    "Vermiculite": '0',
    "Mold": '1'
  }], dtype='object')

  data['Health_Safety_Barrier__c'] = data.apply(combine_hs, axis=1)
  print(data)
  assert data.equals(DataFrame([{
    "Knob and Tube": '1',
    "Asbestos": '0',	
    "Vermiculite": '1',
    "Mold": '1',
    'Health_Safety_Barrier__c': 'Knob & Tube (Major);Vermiculite;Mold/Moisture' 
  },{
    "Knob and Tube": '1',
    "Asbestos": '0',	
    "Vermiculite": '0',
    "Mold": '1',
    'Health_Safety_Barrier__c': 'Knob & Tube (Major);Mold/Moisture'
  },{
    "Knob and Tube": '0',
    "Asbestos": '0',	
    "Vermiculite": '0',
    "Mold": '1',
    'Health_Safety_Barrier__c': 'Mold/Moisture'
  }], dtype='object'))


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
