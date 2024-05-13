# new-snowflake-connect lambda, IAM new-snowflake-role, IAM s3-fifa while making lambda

import snowflake.connector
import boto3
import csv
import os

def lambda_handler(event, context):
    # Snowflake connection parameters
    account = 'xxxxxxxxxx'
    user = 'xxxxx'
    password = 'xxxxxxxxx'
    warehouse = 'COMPUTE_WH'
    database = 'DATA'
    schema = 'PUBLIC'
    snowflake_table = 'PLAYER_INFO_TRANSFER1'

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

    # Create table for player information
    cursor = conn.cursor()
    cursor.execute(f'''
        CREATE TABLE IF NOT EXISTS {snowflake_table} (
            Name VARCHAR PRIMARY KEY,
            Nationality VARCHAR,
            Age INT,
            Rating INT,
            Club VARCHAR,
            "Height(cm)" INT,
            Foot VARCHAR,
            Position VARCHAR,
            "Value(€)" FLOAT,
            "Weekly Wage(€)" FLOAT
        )
    ''')

    # Create table for player statistics
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS PLAYER_STATISTICS (
            Player_Stat_Id INT IDENTITY PRIMARY KEY,
            PAC INT,
            SHO INT,
            PAS INT,
            DRI INT,
            DEF INT,
            PHY INT
        )
    ''')

    # Create table to associate player information with statistics
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS PLAYER_INFO_STATS (
            PLAYER_INFO_STATS INT IDENTITY PRIMARY KEY, 
            Name VARCHAR,
            Player_Stat_Id INT,
            FOREIGN KEY (Name) REFERENCES PLAYER_INFO_TRANSFER (Name),
            FOREIGN KEY (Player_Stat_Id) REFERENCES PLAYER_STATISTICS (Player_Stat_Id)
        )
    ''')

    # S3 client
    s3_client = boto3.client('s3')

    # Download CSV file from S3
    local_file = '/tmp/data.csv'
    s3_client.download_file(s3_bucket, s3_key, local_file)

    # Load data into Snowflake - Player Information table
    with open(local_file, 'r') as file:
        csv_reader = csv.reader(file)
        next(csv_reader)  # Skip header row
        for row in csv_reader:
            cursor.execute(f'''
                INSERT INTO {snowflake_table} (Name, Nationality, Age, Rating, Club, "Height(cm)", Foot, Position, "Value(€)", "Weekly Wage(€)")
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ''', row[:10])  # Ensure only the first 10 elements are used

    # Load data into Snowflake - Player Statistics table
    with open(local_file, 'r') as file:
        csv_reader = csv.reader(file)
        next(csv_reader)  # Skip header row
        for row in csv_reader:
            cursor.execute('''
                INSERT INTO PLAYER_STATISTICS (PAC, SHO, PAS, DRI, DEF, PHY)
                VALUES (%s, %s, %s, %s, %s, %s)
            ''', row[10:16])  # Ensure only elements 10 to 15 are used

    # Load data into Snowflake - Player Information & Statistics association table
    cursor.execute("SELECT MAX(Player_Stat_Id) FROM PLAYER_STATISTICS")
    max_stat_id = cursor.fetchone()[0] or 0  # Handle case when table is empty
    with open(local_file, 'r') as file:
        csv_reader = csv.reader(file)
        next(csv_reader)  # Skip header row
        for row in csv_reader:
            cursor.execute("""
                INSERT INTO PLAYER_INFO_STATS (Name, Player_Stat_Id)
                VALUES (%s, %s)
            """, (row[0], max_stat_id))

    cursor.close()
    conn.close()

    # Clean up downloaded file
    os.remove(local_file)

    return {
        'statusCode': 200,
        'body': 'Data loaded into Snowflake successfully.'
    }
