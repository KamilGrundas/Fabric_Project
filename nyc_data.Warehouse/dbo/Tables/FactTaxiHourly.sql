CREATE TABLE [dbo].[FactTaxiHourly] (

	[pickup_datetime] datetime2(0) NULL, 
	[dropoff_datetime] datetime2(0) NULL, 
	[pickup_hour] int NULL, 
	[trip_duration_minutes] decimal(10,4) NULL, 
	[PUBoroughID] int NULL, 
	[DOBoroughID] int NULL, 
	[trip_distance] float NULL, 
	[fare_amount] decimal(10,2) NULL, 
	[tip_amount] decimal(10,2) NULL, 
	[total_amount] decimal(10,2) NULL, 
	[PickupDateKey] int NULL, 
	[DropoffDateKey] int NULL
);