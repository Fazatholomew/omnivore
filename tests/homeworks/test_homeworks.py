from omnivore.homeworks.homeworks import homeworks, rename_and_merge, combine_hs
from .homeworks_data import input_new_data, input_old_data, output_data
from pandas import isna, DataFrame
import pytest


def test_homeworks_hsMapper():
    data = DataFrame(
        [
            {"Knob and Tube": "1", "Asbestos": "0", "Vermiculite": "1", "Mold": "1"},
            {"Knob and Tube": "1", "Asbestos": "0", "Vermiculite": "0", "Mold": "1"},
            {"Knob and Tube": "0", "Asbestos": "0", "Vermiculite": "0", "Mold": "1"},
        ],
        dtype="object",
    )

    data["Health_Safety_Barrier__c"] = data.apply(combine_hs, axis=1)
    assert data.equals(
        DataFrame(
            [
                {
                    "Knob and Tube": "1",
                    "Asbestos": "0",
                    "Vermiculite": "1",
                    "Mold": "1",
                    "Health_Safety_Barrier__c": "Knob & Tube (Major);Vermiculite;Mold/Moisture",
                },
                {
                    "Knob and Tube": "1",
                    "Asbestos": "0",
                    "Vermiculite": "0",
                    "Mold": "1",
                    "Health_Safety_Barrier__c": "Knob & Tube (Major);Mold/Moisture",
                },
                {
                    "Knob and Tube": "0",
                    "Asbestos": "0",
                    "Vermiculite": "0",
                    "Mold": "1",
                    "Health_Safety_Barrier__c": "Mold/Moisture",
                },
            ],
            dtype="object",
        )
    )


data = rename_and_merge(input_old_data, input_new_data)
result = homeworks(data)

result_lists = result.to_dict(orient="records")
output_lists = output_data.to_dict(orient="records")


@pytest.mark.parametrize(
    "output,result",
    [(output_lists[i], result_lists[i]) for i in range(len(output_lists))],
)
def test_homeworks_processing_function(output, result):
    for column in output.keys():
        expected_value = output[column]  # type: ignore
        current_value = result[column]  # type: ignore
        if isna(expected_value):
            assert (
                isna(current_value) or len(current_value) == 0 or current_value == "nan"
            ), f"\nColumn = '{column}'\nHomeworks code = {current_value}\nExpected value = {expected_value}\nHomeworks ID = {output['ID_from_HPC__c']}\n"
        else:
            assert (
                expected_value == current_value
            ), f"\nColumn = '{column}'\nHomeworks code = {current_value}\nExpected value = {expected_value}\nHomeworks ID = {output['ID_from_HPC__c']}\n"
