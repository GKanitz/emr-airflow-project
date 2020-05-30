# Project description

In order to support thr growth of cities with in the USA a datalake was forged allowing marketeers to easily inspect historic immigration data.

- Understand tourism in different seasons beased on temperature data and visa types.
- Investigation of general immigration to metropolitain areas by vida type and explore corellation with weather related factors.

Using the provided data sources, a Data Lake was made available on S3 to query for weather and demographics of popular immigration destinations, which can be useful for the development of metropolitain regions.

# Data sources

- I94 Immigration data records partitioned by month of every year, obtained from the US National Tourism and Trade Office [Source](https://travel.trade.gov/research/reports/i94/historical/2016.html).
- World temperature Data recordings of cities around the world, obtained from Kaggle [Source](https://www.kaggle.com/berkeleyearth/climate-change-earth-surface-temperature-data).
- US City Demographic of US states obtained from OpenSoft [Source](https://public.opendatasoft.com/explore/dataset/us-cities-demographics/export/).
- Aiport Code table associating airport codes to their respective cities, obtained from [Source](https://datahub.io/core/airport-codes#data).

# Data Lake designs

The data lake was formed as a star layout and stored to S3 to enabale: easy mutaion of schema, make it extendible and easily accessible.

## Tables

# AWS Infrastructure

- The AWS infrastructure is set up according to this [tutorial](https://aws.amazon.com/blogs/big-data/build-a-concurrent-data-orchestration-pipeline-using-amazon-emr-and-apache-livy/)
- Upload the CloudFormation script to create the resources, such as EC2 instance, RSD database for Airflow, security groups, S3 bucket
- Connect to the airflow server and execute dag

# ETL

## Possible errors

- Livy session NOT started, restart EMR, restart Airflow scheduler

# Scenarios

- Data increase by 100x. read > write. write > read
  - Redshift: Analytical database, optimized for aggregation, also good performance for read-heavy workloads
  - Cassandra: Is optimized for writes, can be used to write online transactions as it comes in, and later aggregated into analytics tables in Redshift
  - Increase EMR cluster size to handle bigger volume of data

* Pipelines would be run on 7am daily. how to update dashboard? would it still work?
  - DAG retries, or send emails on failures
  - daily intervals with quality checks
  - if checks fail, then send emails to operators, freeze dashboard, look at DAG logs to figure out what went wrong

- Make it available to 100+ people
  - Redshift with auto-scaling capabilities and good read performance
  - Cassandra with pre-defined indexes to optimize read queries
