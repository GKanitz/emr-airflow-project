import os
from pyspark.sql.types import (
    StructType,
    StructField,
    DoubleType,
    IntegerType,
    StringType,
)

demographicsSchema = StructType(
    [
        StructField("city", StringType()),
        StructField("state", StringType()),
        StructField("median_age", DoubleType()),
        StructField("male_population", StringType()),
        StructField("female_population", StringType()),
        StructField("total_population", IntegerType()),
        StructField("number_of_veterans", IntegerType()),
        StructField("number_of_foreign_born", IntegerType()),
        StructField("average_household_size", DoubleType()),
        StructField("state_code", StringType()),
        StructField("race", StringType()),
        StructField("count", IntegerType()),
    ]
)

df_demographics = spark.read.csv(
    os.path.join(source_bucket, "us-cities-demographics.csv"),
    header="true",
    sep=";",
    schema=demographicsSchema,
)
print("df_demographics ", df_demographics.count())

# clean the data - no duplicates 2891 records before and after clean-up
df_demographics_clean = df_demographics.filter(
    df_demographics.state.isNotNull()
).dropDuplicates(subset=["state", "city", "race"])

# perform data quality checks
print("df_demographics_clean ", df_demographics_clean.count())
print(df_demographics_clean.show(5, truncate=False))
df_demographics_clean.printSchema()

dirpath = os.path.join(output_bucket, "us_cities_demographics.parquet")
print("Partitioning data")
df_demographics_clean_partitioned = df_demographics_clean.repartition("state")
print("Writing parquet data")
df_demographics_clean_partitioned.write.mode("overwrite").parquet(dirpath)
