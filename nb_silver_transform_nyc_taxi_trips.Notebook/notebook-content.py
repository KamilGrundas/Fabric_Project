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

from pyspark.sql.functions import col, lit, unix_timestamp

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

green_df = spark.read.table("nyc_green_taxi_trips_raw")
yellow_df = spark.read.table("nyc_yellow_taxi_trips_raw")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

green_clean = (
    spark.read.table("nyc_green_taxi_trips_raw")
    .select(
        col("lpep_pickup_datetime").alias("pickup_datetime"),
        col("lpep_dropoff_datetime").alias("dropoff_datetime"),
        col("PULocationID"),
        col("DOLocationID"),
        col("passenger_count"),
        col("trip_distance"),
        col("fare_amount"),
        col("tip_amount"),
        col("total_amount"),
        col("payment_type")
    )
    .withColumn(
        "trip_duration_minutes",
        (unix_timestamp("dropoff_datetime") - unix_timestamp("pickup_datetime")) / 60
    )
    .withColumn("taxi_type", lit("green"))
)



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

yellow_clean = (
    spark.read.table("nyc_yellow_taxi_trips_raw")
    .select(
        col("tpep_pickup_datetime").alias("pickup_datetime"),
        col("tpep_dropoff_datetime").alias("dropoff_datetime"),
        col("PULocationID"),
        col("DOLocationID"),
        col("passenger_count"),
        col("trip_distance"),
        col("fare_amount"),
        col("tip_amount"),
        col("total_amount"),
        col("payment_type")
    )
    .withColumn(
        "trip_duration_minutes",
        (unix_timestamp("dropoff_datetime") - unix_timestamp("pickup_datetime")) / 60
    )
    .withColumn("taxi_type", lit("yellow"))
)



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

taxi_trips_clean = green_clean.unionByName(yellow_clean)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

taxi_trips_dedup = taxi_trips_clean.dropDuplicates([
    "pickup_datetime",
    "dropoff_datetime",
    "PULocationID",
    "DOLocationID",
    "taxi_type"
])


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

spark.sql("""
CREATE TABLE IF NOT EXISTS silver_taxi_trips (
    pickup_datetime TIMESTAMP,
    dropoff_datetime TIMESTAMP,
    PULocationID INT,
    DOLocationID INT,
    passenger_count BIGINT,
    trip_distance DOUBLE,
    fare_amount DOUBLE,
    tip_amount DOUBLE,
    total_amount DOUBLE,
    payment_type BIGINT,
    trip_duration_minutes DOUBLE,
    taxi_type STRING
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
    taxi_trips_dedup
    .select(
        "pickup_datetime",
        "dropoff_datetime",
        "PULocationID",
        "DOLocationID",
        "passenger_count",
        "trip_distance",
        "fare_amount",
        "tip_amount",
        "total_amount",
        "payment_type",
        "trip_duration_minutes",
        "taxi_type"
    )
    .write
    .mode("append")
    .saveAsTable("silver_taxi_trips")
)



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
