import json
import boto3
import pandas as pd
import io
import csv

s3 = boto3.client('s3')

def transform_cleanse_data(file_obj):
    try:
        # Read CSV file into DataFrame
        df = pd.read_csv(file_obj['Body'])

        # List of current Premier League teams
        clubs = [
            "Arsenal", "Aston Villa", "Brentford", "Brighton & Hove Albion",
            "Burnley", "Chelsea", "Crystal Palace", "Everton", "Leeds United",
            "Leicester City", "Liverpool", "Manchester City", "Manchester United",
            "Newcastle United", "Norwich City", "Southampton", "Tottenham Hotspur",
            "Watford", "West Ham United", "Wolverhampton Wanderers"
        ]

        # Drop Columns
        df = df.drop(columns=['Name','ID','photoUrl','playerUrl','POT','Contract','Weight','Positions','BOV','Joined','Loan Date End','Hits'])
        df = df.drop(columns=df.columns[10:59])

        # Replace Values 
        df['Club'] = df['Club'].str.replace('\n', '')
        df['Value'] = df['Value'].str.replace('€', '')
        df['Wage'] = df['Wage'].str.replace('€', '')
        df['Wage'] = df['Wage'].str.replace('K', '')
        df['Height'] = df['Height'].str.replace('cm', '')
        df['Height'] = df['Height'].str.replace('"', '')

        # Define a function to convert strings like '130k' and '3M' to numeric values
        def convert_to_numeric(value):
            value = value.upper()  # Convert to uppercase to handle both 'k' and 'M'
            if 'K' in value:
                return float(value.replace('K', '')) * 1000
            elif 'M' in value:
                return float(value.replace('M', '')) * 1000000
            else:
                return float(value)

        # Apply the function to the Value
        df['Value'] = df['Value'].apply(convert_to_numeric)

        # Convert Wage to float
        df['Wage'] = df['Wage'].astype(float)

        df['Wage'] = df['Wage'] * 1000

        # Function to format numbers with commas
        def format_with_commas(value):
            return '{:,.1f}'.format(value)

        # Rename Columns
        df.rename(columns={'↓OVA': 'Rating', 'LongName': 'Name', 'Best Position': 'Position', 'Preferred Foot': 'Foot', 'Value': 'Value(€)', 'Wage': 'Weekly Wage(€)', 'Height': 'Height(cm)'}, inplace=True)

        # Filter for Premier League teams
        df = df[df['Club'].isin(clubs)]

        # Convert Height from feet to cm
        def feet_to_cm(height):
            if "'" in height:  # Check if the height is in feet
                feet, inches = height.split("'")  # Split feet and inches
                total_inches = int(feet) * 12 + int(inches)  # Convert feet and inches to total inches
                cm = total_inches * 2.54  # Convert inches to cm
                return cm
            else:
                return int(height)  # If already in cm, return as it is

        # Apply the function to the height column
        df['Height(cm)'] = df['Height(cm)'].apply(feet_to_cm)

        # Convert DataFrame into list of dictionaries
        list_of_dicts = df.to_dict(orient='records')

        return list_of_dicts
    except Exception as e:
        print("Error in transform_cleanse_data:", e)
        raise

def lambda_handler(event, context):
    try:
        for record in event['Records']:
            # Extract bucket name and file name from S3 event
            source_bucket_name = 'fifa-raw-bucket'
            file_key = 'fifa21 raw data v2.csv'
            
            # Read the CSV file from S3
            response = s3.get_object(Bucket=source_bucket_name, Key=file_key)
            
            # Process the CSV file
            list_of_dicts = transform_cleanse_data(response)
            
            # Convert list of dictionaries back to CSV format
            csv_buffer = io.StringIO()
            writer = csv.DictWriter(csv_buffer, fieldnames=list_of_dicts[0].keys())
            writer.writeheader()
            writer.writerows(list_of_dicts)
            csv_buffer.seek(0)
            
            # Upload the modified CSV as a new file to another S3 bucket
            target_bucket_name = 'fifa-clean-bucket'
            new_file_name = 'cleaned_' + file_key  # Prefix 'changes_' to original file name
            s3.put_object(Bucket=target_bucket_name, Key=new_file_name, Body=csv_buffer.getvalue(), ContentType='text/csv')

        return {
            'statusCode': 200,
            'body': 'Processed uploaded CSV files successfully'
        }
    except Exception as e:
        print("Error in lambda_handler:", e)
        return {
            'statusCode': 500,
            'body': 'Error processing uploaded CSV files'
        }
