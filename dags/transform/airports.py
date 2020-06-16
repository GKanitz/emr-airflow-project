"""clean and extract airport data"""
import os
from pyspark.sql.functions import col, split
from pyspark.sql.types import (
    StructType,
    StructField,
    DoubleType,
    StringType,
)

airportSchema = StructType(
    [
        StructField("airport_id", StringType()),
        StructField("type", StringType()),
        StructField("name", StringType()),
        StructField("elevation_ft", StringType()),
        StructField("continent", StringType()),
        StructField("iso_country", StringType()),
        StructField("iso_region", StringType()),
        StructField("municipality", StringType()),
        StructField("gps_code", StringType()),
        StructField("iata_code", StringType()),
        StructField("local_code", StringType()),
        StructField("coordinates", StringType()),
    ]
)
source_file = os.path.join(source_bucket, "airport-codes_csv.csv")
df_airport = spark.read.csv(
    source_file, header="true", schema=airportSchema,
).distinct()
print("df_airport ", df_airport.count())

df_airport_coord = (
    df_airport.filter("iso_country == 'US'")
    .withColumn("state", split(col("iso_region"), "-")[1])
    .withColumn("latitude", split(col("coordinates"), ",")[0].cast(DoubleType()))
    .withColumn("longitude", split(col("coordinates"), ",")[1].cast(DoubleType()))
    .drop("coordinates")
    .drop("iso_region")
    .drop("continent")
)

df_airport_coord.createOrReplaceTempView("df_airports")

df_airport_clean = spark.sql(
    """
    select
        airport_id,
        type, name,
        elevation_ft,
        iso_country,
        state,
        municipality,
        gps_code,
        iata_code as airport_code,
        latitude,
        longitude
    from
        df_airports
    where
        iata_code is not null OR
        local_code is not null
    """
)

print("df_airport_clean ", df_airport_clean.count())
print(df_airport_clean.show(5, truncate=False))
df_airport_clean.printSchema()

dirpath = os.path.join(output_bucket, "us_airports.parquet")
df_airport_clean.repartitionByRange(3, "airport_code", "state").write.mode(
    "overwrite"
).parquet(dirpath)
