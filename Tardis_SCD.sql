/*********************************************************************
Author		:Umesh
Purspose	:Performing Incremental Loading Using SCD
Date		:Sep 12,2025
Database	:[TARDISDW_SCD]
**********************************************************************/
use [TARDISDW_SCD]
go

--Insert the Xref tables:
select * from [dbo].[AssuredXRef]
select * from [dbo].[BranchXRef]
select * from [dbo].[BrokerXRef]
select * from [dbo].[CompanyXRef]
select * from [dbo].[ProductXRef]
select * from [dbo].[SectionTypeXRef]
select * from [dbo].[StatusXRef]
select * from [dbo].[UnderwriterXRef]

--Run DimTimeTables Script as well {Insert from NewTimeDimScript}
select * from  Dimension.Date	--Use this

select * from  [Dimension].[Time]
--Column_name		Type	Computed	 Length	 Prec	Scale	Nullable	TrimTrailingBlanks	FixedLenNullInSource	Collation
--Date		      datetime	  no			8		  	     	  no				(n/a)				(n/a)				NULL

select* from TARDIS.[dbo].[Time]


sp_help'[Dimension].[Time]'
--Here we have :
/*
		Type 0   -   Changes treated as Errors [Once it is given nobody can modify]  Ex: DOB,NAME  {Read only data}
		Type 1	 -   Overwriting data
		Type 2	 -	 Historical data		
*/

--For now we are using Type 1:
/*
	For YearOfAccount		Changing Attribute
		Mater Reference		Changing Attribute
		Domicile			Changing Attribute
		Inception Date		Changing Attribute
		ExpiryDate			Changing Attribute
*/

select *  from [TARDISDW_SCD].[Dimension].[Policy]			--379  Pastdate
select *  from [TARDISDW_SCD].[Dimension].[PolicySection]	--365  Present date
select *  from [TARDISDW_SCD].[Dimension].[PolicyCoverage] --365  Present date
select *  from [TARDISDW_SCD].[Dimension].[TransactionType]--1	   Past date
select *  from [TARDISDW_SCD].[Dimension].[RevenueType]	--2	   Past date
select *  from [TARDISDW_SCD].[Dimension].[Currency]		--3	   Past date



select *  from [TARDISDW_SCD].Fact.Premium
select *  from [TARDISDW_SCD].Fact.Limit
select *  from [TARDISDW_SCD].fact.Deduction








--Get the time
select* from TARDIS.[dbo].[Time]
select * from TARDISDW_SCD.Dimension.Time



insert into TARDISDW_SCD.Dimension.Time(TimeID,Date,YearID,YearDisplay,HalfYearID,HalfYearDisplay,QuarterID,QuarterDisplay,MonthID,MonthDisplay,SourceSystemID
,DateCreated,DateExpired,DateUpdated,CurrentYN,IsWeekDay)

select TimeID,Date,YearID,YearDisplay,HalfYearID,HalfYearDisplay,QuarterID,QuarterDisplay,MonthID,MonthDisplay,SourceSystemID
,DateCreated,DateExpired,DateUpdated,CurrentYN,IsWeekDay
from TARDIS.[dbo].[Time]







-- What are the issue you are getting in this project:
--1. Data Conversion issues
--2. Mapping is not done properly
--3. lookup tables doesn't have data 


-- If you are facing any type of time issues .There is one more solution without loading the data to TARDISDW_SCD.Dimension.Time(table)
-- Use "XrefData- DW Inserting Data"
