CREATE TABLE [dbo].[silver_taxi_trips] (

	[pickup_datetime] datetime2(6) NULL, 
	[dropoff_datetime] datetime2(6) NULL, 
	[PULocationID] int NULL, 
	[DOLocationID] int NULL, 
	[passenger_count] bigint NULL, 
	[trip_distance] float NULL, 
	[fare_amount] float NULL, 
	[tip_amount] float NULL, 
	[total_amount] float NULL, 
	[payment_type] bigint NULL, 
	[trip_duration_minutes] float NULL, 
	[taxi_type] varchar(8000) NULL
);