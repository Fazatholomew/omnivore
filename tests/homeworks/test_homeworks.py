from omnivore.homeworks.homeworks import homeworks
from .homeworks_data import input_data, output_data
from pandas import read_csv
from typing import cast
from os import getenv
import pytest

def test_homeworks_processing_function():
    raw_data = read_csv(cast(str, getenv('HOMEWORKS_DATA_URL')), dtype='object')
    wx_data = read_csv(cast(str, getenv('HOMEWORKS_COMPLETED_DATA_URL')), dtype='object')
    result = homeworks(raw_data, wx_data)
    result.to_pickle('./homeworks processed dec 10.pkl')
    # for i in range(len(output_data) - 1):
    #   for column in output_data.columns:
    #     expected_value = output_data.iloc[i][column]
    #     current_value = result.iloc[i][column]
    #     if isna(expected_value):
    #       assert isna(current_value), f"\nColumn = '{column}'\nNeeeco code = {current_value}\nExpected value = {expected_value}\nNeeeco ID = {output_data.iloc[i]['ID_from_HPC__c']}\n"
    #     else:
    #       assert expected_value == current_value, f"\nColumn = '{column}'\nNeeeco code = {current_value}\nExpected value = {expected_value}\nNeeeco ID = {output_data.iloc[i]['ID_from_HPC__c']}\n"
