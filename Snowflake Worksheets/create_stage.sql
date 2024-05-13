 // Create stage object with integration object & file format object
CREATE OR REPLACE stage DATA.DATA_SCHEMA.csv_folder
    URL = 's3://snowflake-s3-bucket-12/csv/'
    STORAGE_INTEGRATION = s3_int
    FILE_FORMAT = DATA.DATA_SCHEMA.csv_fileformat