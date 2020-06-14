""" Load the immigration data and write the parquet files to the S3 bucket."""
import os
import datetime as dt
from pyspark.sql.functions import udf

df_spark = spark.read.format("com.github.saurfang.sas.spark").load(
    os.path.join(source_bucket, f"i94_{month_year}_sub.sas7bdat")
)

# get datetime from arrdate column value
get_date = udf(
    lambda x: (dt.datetime(1960, 1, 1).date() + dt.timedelta(x)).isoformat()
    if x
    else None
)
df_spark = df_spark.withColumn("arrdate", get_date(df_spark.arrdate))
df_spark.createOrReplaceTempView("df_spark")
df_usa_states = spark.read.parquet(os.path.join(output_bucket, "stateCodes.parquet"))
df_usa_states.createOrReplaceTempView("df_usa_states")
df_usa_visas = spark.read.parquet(os.path.join(output_bucket, "visaCodes.parquet"))
df_usa_visas.createOrReplaceTempView("df_usa_visas")
df_tavel_mode = spark.read.parquet(
    os.path.join(output_bucket, "transportCodes.parquet")
)
df_tavel_mode.createOrReplaceTempView("df_tavel_mode")

# all missing states are 99, get the arrival mode and the visa type from the mappings
df_immigration_clean = spark.sql(
    """
    select
        i.i94yr as year,
        i.i94mon as month,
        i.i94cit as birth_country,
        i.i94res as residence_country,
        i.i94port as port,
        i.arrdate as arrival_date,
        coalesce(m.Transport, 'Not reported') as arrival_mode,
        coalesce(c.State, '99') as us_state,
        i.depdate as departure_date,
        i.i94bir as repondent_age,
        coalesce(v.Visa, 'Other') as visa_type_code,
        i.dtadfile as date_added,
        i.visapost as visa_issued_department,
        i.occup as occupation,
        i.entdepa as arrival_flag,
        i.entdepd as departure_flag,
        i.entdepu as update_flag,
        i.matflag as match_arrival_departure_fag,
        i.biryear as birth_year,
        i.dtaddto as allowed_date,
        i.insnum as ins_number,
        i.airline as airline,
        i.admnum as admission_number,
        i.fltno as flight_number,
        i.visatype as visa_type
    from
        df_spark i
    left join
        df_usa_states c on i.i94addr=c.Code
    left join
        df_usa_visas v on i.i94visa=v.Code
    left join
        df_tavel_mode m on i.i94mode=m.Code
"""
)
# perform data quality checks
print("df_immigration_clean ", df_immigration_clean.count())
print(df_immigration_clean.show(5, truncate=False))
df_immigration_clean.printSchema()

# write data to parquet and partition by year and and month
dirpath = os.path.join(output_bucket, "immigration_us_state.parquet")
df_immigration_clean_repartitioned = df_immigration_clean.repartition(
    "year", "month", "us_state"
)
df_immigration_clean_repartitioned.write.mode("overwrite").parquet(dirpath)

dirpath = os.path.join(output_bucket, "immigration_arrival_mode.parquet")
df_immigration_clean_repartitioned = df_immigration_clean.repartition(
    "year", "month", "arrival_mode", "port"
)
df_immigration_clean_repartitioned.write.mode("overwrite").parquet(dirpath)
