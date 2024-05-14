# FIFA-ETL-PIPELINE-AWS-SNOWFLAKE

## Overview 
EA Sports provided me with a raw, disorganized CSV file containing a comprehensive list of programming available on their platform. My task is to develop a automated ETL (Extract, Transform, Load) pipeline to clean, transform, and load the CSV file onto both the AWS and Snowflake platforms.
## Aims
This project serves as a platform for me to demonstrate my proficiency in leveraging tools such as Pandas and Python, along with my burgeoning familiarity with AWS and Snowflake platforms. It marks my initial foray into utilizing some of these features, offering an opportunity for growth and exploration as I navigate the intricacies of data manipulation, cloud computing, and database management. Through this endeavor, I aim to not only accomplish the task at hand but also to broaden my skill set and deepen my understanding of these powerful technologies.

## Technologies 
1. Python (Pandas)
2. AWS (S3 Buckets, Lambda Functions, Cloudwatch, IAM Roles) 
3. SNOWFLAKE (SQL Worksheets, Databases)
## Set Up
### AWS 
1. Create S3 Buckets
2. Create IAM Policy
3. Create Lambdas

### SNOWFLAKE
1. Create database.
2. Create Schema.
3. Create Tables.
4. Create Fileformat.
5. Create Stage. 
6. Create Intergration Object. 

## Future Implementation 

For future implementations of my ETL pipeline on AWS and Snowflake, I'm excited about integrating SQS (Simple Queue Service) and Snowpipe into the architecture. With SQS, I can efficiently queue up data processing tasks, enabling scalable and asynchronous processing. This decoupling of components not only enhances scalability but also boosts reliability by redundantly storing messages across multiple availability zones. On the Snowflake side, incorporating Snowpipe streamlines continuous data loading from cloud storage into Snowflake tables, ensuring real-time or near-real-time updates without manual intervention. This automation not only reduces complexity and cost but also maintains data freshness, ultimately enhancing the overall performance and efficiency of my ETL pipeline.

## Architecture 
![Fifa Snowflake Pipeline.jpg](<Fifa Snowflake Pipeline.jpg>)

## Snowflake Schema 
![FIFA SNOWFLAKE SCHEMA.png](<FIFA SNOWFLAKE SCHEMA.png>)