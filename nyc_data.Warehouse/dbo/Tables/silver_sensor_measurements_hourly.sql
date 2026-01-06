CREATE TABLE [dbo].[silver_sensor_measurements_hourly] (

	[sensor_id] int NULL, 
	[datetime_utc] datetime2(6) NULL, 
	[datetime_local] datetime2(6) NULL, 
	[value] float NULL
);