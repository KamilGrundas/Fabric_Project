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
    IntegerType
)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

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
