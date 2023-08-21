from omnivore.revise.revise import revise, merge_columns, merge_file_revise

from .revise_data import hea_data, wx_data, output_data
from pandas import isna, DataFrame
from pandas.testing import assert_frame_equal


def test_revise_merge_columns():
    data = DataFrame(
        [
            {"Name_x": "1", "Name_y": "0", "date": "hehehehe"},
            {"Name_x": None, "Name_y": "0", "date": "hehehehe"},
            {"Name_x": "0", "Name_y": None, "date": "hehehehe"},
        ],
        dtype="object",
    )

    assert_frame_equal(
        merge_columns(data, ["Name"]),
        DataFrame(
            [
                {"Name_x": "1", "Name_y": "0", "date": "hehehehe", "Name": "1"},
                {"Name_x": None, "Name_y": "0", "date": "hehehehe", "Name": "0"},
                {"Name_x": "0", "Name_y": None, "date": "hehehehe", "Name": "0"},
            ],
            dtype="object",
        ),
    )

    assert_frame_equal(
        merge_columns(data, ["Name"], "y", "x"),
        DataFrame(
            [
                {"Name_x": "1", "Name_y": "0", "date": "hehehehe", "Name": "0"},
                {"Name_x": None, "Name_y": "0", "date": "hehehehe", "Name": "0"},
                {"Name_x": "0", "Name_y": None, "date": "hehehehe", "Name": "0"},
            ],
            dtype="object",
        ),
    )


def test_Revise_processing_function():
    data = merge_file_revise(hea_data, wx_data)
    result = revise(data)
    for i in range(len(output_data)):
        for column in output_data.columns:
            expected_value = output_data.iloc[i][column]  # type: ignore
            current_value = result.iloc[i][column]  # type: ignore
            if isna(expected_value):
                assert (
                    isna(current_value)
                    or len(current_value) == 0
                    or current_value == "nan"
                ), f"\nColumn = '{column}'\nRevise code = {current_value}\nExpected value = {expected_value}\nRevise ID = {output_data.iloc[i]['ID_from_HPC__c']}\n"
            else:
                assert (
                    expected_value == current_value
                ), f"\nColumn = '{column}'\nRevise code = {current_value}\nExpected value = {expected_value}\nRevise ID = {output_data.iloc[i]['ID_from_HPC__c']}\n"
