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
from pyspark.sql.types import StructType, StructField, DoubleType, StringType


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

%run nb_utils_config

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def reverse_geocode(lat, lon):
    url = "https://geocode.maps.co/reverse"
    params = {
        "lat": lat,
        "lon": lon,
        "api_key": GEOCODE_API_KEY
    }

    try:
        r = requests.get(url, params=params, timeout=20)
        if r.status_code != 200:
            return None
        return r.json()
    except Exception:
        return None

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

raw_df = spark.read.table("bronze_openaq_locations_raw")

rows = []

for r in raw_df.select("raw_json").collect():
    try:
        loc = json.loads(r.raw_json)
    except Exception:
        continue

    coords = loc.get("coordinates") or {}
    lat = coords.get("latitude")
    lon = coords.get("longitude")

    if lat is None or lon is None:
        continue

    geo = reverse_geocode(lat, lon)
    time.sleep(1)  # rate limit

    if geo:
        rows.append(
            Row(
                lat=float(lat),
                lon=float(lon),
                json_raw=json.dumps(geo)
            )
        )

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

schema = StructType([
    StructField("lat", DoubleType(), False),
    StructField("lon", DoubleType(), False),
    StructField("json_raw", StringType(), False),
])

if rows:
    df = spark.createDataFrame(rows, schema)

    spark.sql("""
    CREATE TABLE IF NOT EXISTS locations_geocodes_raw (
        lat DOUBLE,
        lon DOUBLE,
        json_raw STRING
    )
    USING DELTA
    """)

    df.write.mode("append").saveAsTable("bronze_geocodes_locations_raw")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
