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

from pyspark.sql.functions import col, from_json, to_timestamp, from_utc_timestamp

from pyspark.sql.types import StructType, StructField, StringType, DoubleType

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

schema = StructType([
    StructField("period", StructType([
        StructField("datetimeTo", StructType([
            StructField("utc", StringType()),
            StructField("local", StringType())
        ]))
    ])),
    StructField("value", DoubleType())
])

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

bronze_df = spark.read.table("bronze_sensor_measurements_hourly_raw")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


parsed = bronze_df.select(
    col("sensor_id").cast("int"),
    from_json(col("raw_json"), schema).alias("j")
)

silver_df = parsed.select(
    col("sensor_id"),
    to_timestamp(col("j.period.datetimeTo.utc")).alias("datetime_utc"),
    from_utc_timestamp(
        to_timestamp(col("j.period.datetimeTo.utc")),
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

(
    silver_df
    .dropDuplicates(["sensor_id", "datetime_utc"])
    .write
    .mode("overwrite")
    .saveAsTable("silver_sensor_measurements_hourly")
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
