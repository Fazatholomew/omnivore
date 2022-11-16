from pandas import read_csv

input_data = read_csv('./tests/neeeco/neeeco test input.csv', dtype='object')
input_wx_data = read_csv('./tests/neeeco/Neeeco Wx Input.csv', dtype='object')
output_data = read_csv('./tests/neeeco/neeeco_test.csv', dtype='object')