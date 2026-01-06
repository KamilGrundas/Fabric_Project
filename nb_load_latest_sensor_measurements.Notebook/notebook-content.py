# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "5a8fce1b-a162-4453-8498-21708537897d",
# META       "default_lakehouse_name": "nyc_lh_data",
# META       "default_lakehouse_workspace_id": "9cd4ffe5-68b7-4261-8163-b56bf0bc32ea",
# META       "known_lakehouses": [
# META         {
# META           "id": "5a8fce1b-a162-4453-8498-21708537897d"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

import requests
import time
import json
from datetime import datetime
from pyspark.sql import Row

from pyspark.sql.functions import (
    col,
    from_json,
    to_timestamp,
    from_utc_timestamp
)
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    DoubleType,
    IntegerType,
    ArrayType
)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Bronze phase

# MARKDOWN ********************

# ## Sensor Measurements

# CELL ********************

%run nb_utils_config

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

BASE_URL = "https://api.openaq.org/v3"

HEADERS = {
    "X-API-Key": OPENAQ_API_KEY
}


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

locations_df = (
    spark.read
    .table("silver_openaq_locations")
    .select("location_id")
)

location_ids = [r.location_id for r in locations_df.collect()]


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def fetch_latest_for_location(location_id):
    ingestion_hour = int(
        datetime.utcnow().strftime("%Y%m%d%H")
    )
    print(ingestion_hour)

    r = requests.get(
        f"{BASE_URL}/locations/{location_id}/latest",
        headers=HEADERS,
        timeout=60
    )

    if r.status_code != 200:
        print(f"❌ location {location_id} | {r.status_code}")
        return []

    payload = r.json()

    return [
        Row(
            location_id=int(location_id),
            ingestion_hour=ingestion_hour,
            raw_json=json.dumps(result)
        )
        for result in payload.get("results", [])
    ]



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

spark.sql("""
CREATE TABLE IF NOT EXISTS bronze_sensor_measurements_latest_raw (
    location_id     INT,
    ingestion_hour  INT,
    raw_json        STRING
)
USING DELTA
PARTITIONED BY (location_id)
""")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

rows = []

for location_id in location_ids:
    print(f"🔹 location {location_id}")
    rows.extend(fetch_latest_for_location(location_id))
    time.sleep(0.4)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

if rows:
    incoming_df = (
        spark.createDataFrame(rows)
        .select(
            col("location_id").cast("int"),
            col("ingestion_hour").cast("int"),
            col("raw_json")
        )
        .dropDuplicates(["location_id", "ingestion_hour"])
    )

    existing_keys = (
        spark.table("bronze_sensor_measurements_latest_raw")
        .select(
            col("location_id").cast("int"),
            col("ingestion_hour").cast("int")
        )
        .dropDuplicates()
    )

    new_only_df = (
        incoming_df
        .join(
            existing_keys,
            on=["location_id", "ingestion_hour"],
            how="left_anti"
        )
    )

    if not new_only_df.isEmpty():
        (
            new_only_df
            .write
            .mode("append")
            .saveAsTable("bronze_sensor_measurements_latest_raw")
        )


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Weather data

# CELL ********************

WEATHER_URL = (
    "https://api.open-meteo.com/v1/forecast"
    "?latitude=40.7128"
    "&longitude=-74.0060"
    "&hourly=temperature_2m,precipitation,wind_speed_10m"
    "&forecast_hours=1"
    "&timezone=America/New_York"
)

ingestion_hour = int(datetime.utcnow().strftime("%Y%m%d%H"))

response = requests.get(WEATHER_URL, timeout=60)
payload = response.json()

weather_rows = [
    Row(
        ingestion_hour=ingestion_hour,
        raw_json=json.dumps(payload)
    )
]

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

spark.sql("""
CREATE TABLE IF NOT EXISTS bronze_weather_nyc_raw (
    ingestion_hour INT,
    raw_json       STRING
)
USING DELTA
PARTITIONED BY (ingestion_hour)
""")

incoming_weather_df = (
    spark.createDataFrame(weather_rows)
    .select(
        col("ingestion_hour").cast("int"),
        col("raw_json")
    )
)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

existing_weather_keys = (
    spark.table("bronze_weather_nyc_raw")
    .select(col("ingestion_hour").cast("int"))
    .dropDuplicates()
)

new_weather_only_df = (
    incoming_weather_df
    .join(
        existing_weather_keys,
        on="ingestion_hour",
        how="left_anti"
    )
)

if not new_weather_only_df.isEmpty():
    (
        new_weather_only_df
        .write
        .mode("append")
        .saveAsTable("bronze_weather_nyc_raw")
    )

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Silver phase

# MARKDOWN ********************

# ## Sensor measurements

# CELL ********************

latest_schema = StructType([
    StructField("datetime", StructType([
        StructField("utc", StringType()),
        StructField("local", StringType())
    ])),
    StructField("value", DoubleType()),
    StructField("sensorsId", IntegerType()),
    StructField("locationsId", IntegerType())
])

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

bronze_df = spark.read.table("bronze_sensor_measurements_latest_raw")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

parsed = bronze_df.select(
    from_json(col("raw_json"), latest_schema).alias("j")
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

silver_latest_df = parsed.select(
    col("j.sensorsId").cast("int").alias("sensor_id"),
    to_timestamp(col("j.datetime.utc")).alias("datetime_utc"),
    from_utc_timestamp(
        to_timestamp(col("j.datetime.utc")),
        "America/New_York"
    ).alias("datetime_local"),
    col("j.value").alias("value")
)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

silver_latest_df_dedup = silver_latest_df.dropDuplicates(
    ["sensor_id", "datetime_utc"]
)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

(
    silver_latest_df_dedup
    .write
    .mode("append")
    .saveAsTable("silver_sensor_measurements_hourly")
)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Weather

# CELL ********************

weather_schema = StructType([
    StructField("hourly", StructType([
        StructField("time", ArrayType(StringType())),
        StructField("temperature_2m", ArrayType(DoubleType())),
        StructField("precipitation", ArrayType(DoubleType())),
        StructField("wind_speed_10m", ArrayType(DoubleType()))
    ]))
])

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

bronze_weather_df = spark.read.table("bronze_weather_nyc_raw")

parsed_weather = bronze_weather_df.select(
    col("ingestion_hour"),
    from_json(col("raw_json"), weather_schema).alias("j")
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

silver_weather_nyc = parsed_weather.select(
    col("ingestion_hour"),
    to_timestamp(col("j.hourly.time")[0]).alias("datetime_local"),
    col("j.hourly.temperature_2m")[0].alias("temperature_c"),
    col("j.hourly.precipitation")[0].alias("precipitation_mm"),
    col("j.hourly.wind_speed_10m")[0].alias("wind_kmh")
)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

spark.sql("""
CREATE TABLE IF NOT EXISTS silver_weather_nyc (
    ingestion_hour    INT,
    datetime_local    TIMESTAMP,
    temperature_c     DOUBLE,
    precipitation_mm  DOUBLE,
    wind_kmh          DOUBLE
)
USING DELTA
""")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

(
    silver_weather_nyc
    .dropDuplicates(["ingestion_hour"])
    .write
    .mode("append")
    .saveAsTable("silver_weather_nyc")
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
