"""clean and extract airport data"""
import os
from pyspark.sql.functions import col, split
from pyspark.sql.types import (
    StructType,
    StructField as Fld,
    DoubleType as Dbl,
    StringType as Str,
)

# define all variables that are provided externally
# pylint: disable=undefined-variable,self-assigning-variable
spark = spark
source_bucket = source_bucket
output_bucket = output_bucket
# pylint: enable=undefined-variable,self-assigning-variable

airportSchema = StructType(
    [
        Fld("airport_id", Str()),
        Fld("type", Str()),
        Fld("name", Str()),
        Fld("elevation_ft", Str()),
        Fld("continent", Str()),
        Fld("iso_country", Str()),
        Fld("iso_region", Str()),
        Fld("municipality", Str()),
        Fld("gps_code", Str()),
        Fld("iata_code", Str()),
        Fld("local_code", Str()),
        Fld("coordinates", Str()),
    ]
)
df_airport = spark.read.csv(
    os.path.join(source_bucket, "airport-codes_csv.csv"),
    header="true",
    schema=airportSchema,
).distinct()
print("df_airport ", df_airport.count())

df_airport_coord = (
    df_airport.filter("iso_country == 'US'")
    .withColumn("state", split(col("iso_region"), "-")[1])
    .withColumn("latitude", split(col("coordinates"), ",")[0].cast(Dbl()))
    .withColumn("longitude", split(col("coordinates"), ",")[1].cast(Dbl()))
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
