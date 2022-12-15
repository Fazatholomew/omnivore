from pandas import read_csv

input_new_data = read_csv('./tests/homeworks/homeworks_new_test_input.csv', dtype='object')
input_old_data = read_csv('./tests/homeworks/homeworks_old_test_input.csv', dtype='object')
output_data = read_csv('./tests/homeworks/homework_new_output.csv', dtype='object')