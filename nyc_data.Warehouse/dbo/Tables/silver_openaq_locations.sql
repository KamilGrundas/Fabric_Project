CREATE TABLE [dbo].[silver_openaq_locations] (

	[location_id] int NULL, 
	[name] varchar(8000) NULL, 
	[locality] varchar(8000) NULL, 
	[country_code] varchar(8000) NULL, 
	[latitude] float NULL, 
	[longitude] float NULL, 
	[timezone] varchar(8000) NULL, 
	[provider] varchar(8000) NULL, 
	[road] varchar(8000) NULL, 
	[neighbourhood] varchar(8000) NULL, 
	[suburb] varchar(8000) NULL
);