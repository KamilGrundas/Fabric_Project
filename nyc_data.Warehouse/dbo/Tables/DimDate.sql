CREATE TABLE [dbo].[DimDate] (

	[DateKey] int NULL, 
	[DateValue] date NULL, 
	[Year] int NULL, 
	[MonthNumber] int NULL, 
	[MonthShort] char(3) NULL, 
	[Day] int NULL, 
	[DayOfWeek] int NULL, 
	[DayOfWeekShort] char(3) NULL, 
	[IsWeekend] int NULL
);