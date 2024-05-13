# new-snowflake-connect lambda, IAM new-snowflake-role, IAM s3-fifa while making lambda

import snowflake.connector
import boto3
import csv
import os

def lambda_handler(event, context):
    # Snowflake connection parameters
    account = 'epyxlwf-bz70650'
    user = 'imran3'
    password = 'Hsdaujueacha73597529247942ijvroirjvoi_@'
    warehouse = 'COMPUTE_WH'
    database = 'DATA'
    schema = 'PUBLIC'
    snowflake_table = 'PLAYER_INFO_TRANSFER'

    # S3 connection parameters
    s3_bucket = 'fifa-clean-bucket'
    s3_key = 'cleaned_fifa21 raw data v2.csv'

    # Snowflake connection
    conn = snowflake.connector.connect(
        user=user,
        password=password,
        account=account,
        warehouse=warehouse,
        database=database,
        schema=schema
    )

    # Create table if not exists
    cursor = conn.cursor()
    cursor.execute(f'''
        CREATE TABLE IF NOT EXISTS {snowflake_table} (
            Name VARCHAR,
            Nationality VARCHAR,
            Age INT,
            Rating INT,
            Club VARCHAR,
            "Height(cm)" INT,
            Foot VARCHAR,
            Position VARCHAR,
            "Value(€)" FLOAT,
            "Weekly Wage(€)" FLOAT,
            PAC INT,
            SHO INT,
            PAS INT,
            DRI INT,
            DEF INT,
            PHY INT
        )
    ''')

    # S3 client
    s3_client = boto3.client('s3')

    # Download CSV file from S3
    local_file = '/tmp/data.csv'
    s3_client.download_file(s3_bucket, s3_key, local_file)

    # Load data into Snowflake
    with open(local_file, 'r') as file:
        csv_reader = csv.reader(file)
        next(csv_reader)  # Skip header row
        for row in csv_reader:
            cursor.execute("""
                INSERT INTO {table} (Name, Nationality, Age, Rating, Club, "Height(cm)", Foot, Position, "Value(€)", "Weekly Wage(€)", PAC, SHO, PAS, DRI, DEF, PHY)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """.format(table=snowflake_table), row)

    cursor.close()
    conn.close()

    # Clean up downloaded file
    os.remove(local_file)

    return {
        'statusCode': 200,
        'body': 'Data loaded into Snowflake successfully.'
    }
