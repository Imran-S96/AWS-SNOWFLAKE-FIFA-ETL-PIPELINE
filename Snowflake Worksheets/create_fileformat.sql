# // Create file format object
# CREATE OR REPLACE file format DATA.DATA_SCHEMA.csv_fileformat
#     type = csv
#     field_delimiter = ','
#     skip_header = 1
#     null_if = ('NULL','null')
#     empty_field_as_null = TRUE;