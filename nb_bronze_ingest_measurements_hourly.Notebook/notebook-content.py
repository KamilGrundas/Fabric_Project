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
from datetime import datetime, timedelta
from pyspark.sql import Row
from pyspark.sql.functions import col, to_timestamp
import json

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

BASE_URL = "https://api.openaq.org/v3"

HEADERS = {
    "X-API-Key": OPENAQ_API_KEY
}

START_DATE = datetime(2024, 1, 1)
END_DATE   = datetime(2024, 12, 31)
WINDOW     = timedelta(days=31)   # miesięczne okna

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

sensors_df = spark.read.table("silver_sensors").select("sensor_id")
sensor_ids = [r.sensor_id for r in sensors_df.collect()]

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def fetch_measurements(sensor_id, start_date, end_date):
    params = {
        "datetime_from": start_date.isoformat() + "+00:00",
        "datetime_to": end_date.isoformat() + "+00:00",
        "limit": 1000
    }

    r = requests.get(
        f"{BASE_URL}/sensors/{sensor_id}/measurements/hourly",
        headers=HEADERS,
        params=params,
        timeout=60
    )

    if r.status_code != 200:
        print(f"❌ sensor {sensor_id} | {r.status_code}")
        return []

    return [
    Row(
        sensor_id=int(sensor_id),
        raw_json=json.dumps(m)
    )
    for m in r.json().get("results", [])
]


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

spark.sql("""
CREATE TABLE IF NOT EXISTS bronze_sensor_measurements_hourly_raw (
    sensor_id INT,
    raw_json  STRING
)
USING DELTA
PARTITIONED BY (sensor_id)
""")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

current_start = START_DATE

while current_start < END_DATE:
    current_end = min(current_start + WINDOW, END_DATE)
    print(f"\n📅 {current_start.date()} → {current_end.date()}")

    rows = []

    for sensor_id in sensor_ids:
        print(f"  🔹 sensor {sensor_id}")
        rows.extend(fetch_measurements(sensor_id, current_start, current_end))
        time.sleep(1)

    if rows:
        (
            spark.createDataFrame(rows)
            .withColumn("sensor_id", col("sensor_id").cast("int"))
            .write
            .mode("append")
            .saveAsTable("bronze_sensor_measurements_hourly_raw")
        )


    current_start = current_end

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
