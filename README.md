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
- Then connect to the EC2 instance:

```
sudo su
cd ~/airflow
source ~/.bash_profile
bash start.sh
bash run_dags.sh
```

# ETL

- dag_cluster: start the EMR cluster, and wait for all data transformation is finished, then terminate the cluster

![alt text](imgs/cluster-dag.png)

- dag_normalize: wait for EMR cluster to be ready, then use Apache Livy REST API to create interactive Spark session on the cluster, submit a Spark script to read data from S3, do transformation and write the output to S3
  - This DAG handles normalized tables

![alt text](imgs/normalize_dag.png)

- dag_analytics: wait for EMR cluster to be ready, and that normalized tables are processed, then read normalized tables to create analytics tables, and write to S3
  - This DAG handles immigration data, which is partitioned for 12 months from jan-2016 to dec-2016
  - To re-run this DAG, change the DAG name, then delete the existing DAG from Airflow, and refresh the UI

![alt text](imgs/analytics_dag.png)

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
