CREATE TABLE [dbo].[FactAirQuality] (

	[BoroughID] int NULL, 
	[DateKey] int NULL, 
	[HourKey] int NULL, 
	[value] float NULL, 
	[parameter] varchar(8000) NULL, 
	[unit] varchar(8000) NULL
);