from pandas import read_csv

output_data = read_csv('./tests/vhi/VHI Output.csv', dtype='object')
input_data = read_csv('./tests/vhi/VHI Input.csv', dtype='object')