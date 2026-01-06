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

import json
import time
import requests

from pyspark.sql import Row
from pyspark.sql.functions import col
from pyspark.sql.types import (
    StructType, StructField,
    IntegerType, StringType, DoubleType
)



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def is_nyc(geo):
    if not geo:
        return False

    addr = geo.get("address", {})

    return (
        addr.get("country_code") == "us"
        and addr.get("state") == "New York"
        and (
            addr.get("city") in ("City of New York", "New York")
            or addr.get("county") == "New York County"
        )
    )


def extract_address_fields(geo):
    addr = geo.get("address", {}) if geo else {}
    return (
        addr.get("road"),
        addr.get("neighbourhood"),
        addr.get("suburb")
    )



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def normalize_bronx(value):
    if isinstance(value, str) and value.strip().lower() == "the bronx":
        return "Bronx"
    return value


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

raw_locations = spark.read.table("bronze_openaq_locations_raw")
geocodes = spark.read.table("bronze_geocodes_locations_raw")

geo_map = {
    (r.lat, r.lon): json.loads(r.json_raw)
    for r in geocodes.collect()
}


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

locations_rows = []
sensors_rows = []

for r in raw_locations.select("raw_json").collect():
    try:
        loc = json.loads(r.raw_json)
    except Exception:
        continue

    coords = loc.get("coordinates") or {}
    lat = coords.get("latitude")
    lon = coords.get("longitude")

    if lat is None or lon is None:
        continue

    geo = geo_map.get((float(lat), float(lon)))
    if not is_nyc(geo):
        continue

    road, neighbourhood, suburb = extract_address_fields(geo)

    suburb = normalize_bronx(suburb)

    locations_rows.append(
        Row(
            location_id=loc["id"],
            name=loc.get("name"),
            locality=loc.get("locality"),
            country_code=loc["country"]["code"],
            latitude=lat,
            longitude=lon,
            timezone=loc.get("timezone"),
            provider=loc["provider"]["name"],
            road=road,
            neighbourhood=neighbourhood,
            suburb=suburb
        )
    )

    for s in loc.get("sensors", []):
        sensors_rows.append(
            Row(
                sensor_id=s["id"],
                location_id=loc["id"],
                parameter=s["parameter"]["name"],
                unit=s["parameter"]["units"]
            )
        )


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

spark.sql("""
CREATE TABLE IF NOT EXISTS silver_openaq_locations (
    location_id INT,
    name STRING,
    locality STRING,
    country_code STRING,
    latitude DOUBLE,
    longitude DOUBLE,
    timezone STRING,
    provider STRING,
    road STRING,
    neighbourhood STRING,
    suburb STRING
)
USING DELTA
""")

spark.sql("""
CREATE TABLE IF NOT EXISTS silver_sensors (
    sensor_id INT,
    location_id INT,
    parameter STRING,
    unit STRING
)
USING DELTA
""")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

locations_schema = StructType([
    StructField("location_id", IntegerType(), False),
    StructField("name", StringType(), True),
    StructField("locality", StringType(), True),
    StructField("country_code", StringType(), True),
    StructField("latitude", DoubleType(), True),
    StructField("longitude", DoubleType(), True),
    StructField("timezone", StringType(), True),
    StructField("provider", StringType(), True),
    StructField("road", StringType(), True),
    StructField("neighbourhood", StringType(), True),
    StructField("suburb", StringType(), True),
])


if locations_rows:
    locations_df = spark.createDataFrame(
    locations_rows,
    schema=locations_schema
)


    locations_df = locations_df.dropDuplicates(["location_id"])

    existing_locations = spark.table("silver_openaq_locations").select("location_id")

    new_locations = locations_df.join(
        existing_locations,
        on="location_id",
        how="left_anti"
    )

    new_locations.write.mode("append").saveAsTable("silver_openaq_locations")


if sensors_rows:
    sensors_df = (
        spark.createDataFrame(sensors_rows)
        .withColumn("sensor_id", col("sensor_id").cast("int"))
        .withColumn("location_id", col("location_id").cast("int"))
        .withColumn("parameter", col("parameter").cast("string"))
        .withColumn("unit", col("unit").cast("string"))
        .select(
            "sensor_id",
            "location_id",
            "parameter",
            "unit"
        )
    )

    sensors_df = sensors_df.dropDuplicates(["sensor_id"])

    existing_sensors = spark.table("silver_sensors").select("sensor_id")

    new_sensors = sensors_df.join(
        existing_sensors,
        on="sensor_id",
        how="left_anti"
    )

    new_sensors.write.mode("append").saveAsTable("silver_sensors")



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
