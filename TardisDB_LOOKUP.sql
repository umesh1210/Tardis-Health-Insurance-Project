/*********************************************************************
Author		:Umesh Kumar
Purspose	:Performing Incremental Loading Using TARDISDW_LookUP
Date		:Sep 6,2025
Database	:[TARDISDW_LookUP]
**********************************************************************/
SELECT * FROM Tardis_Stage.[dbo].[SSIS_Logs]	--Custom Logging
SELECT * FROM Tardis_Stage.[dbo].[sysssislog]	--Readymade Logging

USE [TARDISDW_LookUP]
GO

-- Dimension Tables [6]
select * from [Dimension].[Policy]			--379
select * from [Dimension].[PolicySection]	--365
select * from [Dimension].[PolicyCoverage]	--365
select * from [Dimension].[TransactionType]	--1
select * from [Dimension].[RevenueType]		--2
select * from [Dimension].[Currency]		--3

--Fact Tables [3]
select * from [Fact].[Premium]  --365
select * from [Fact].[Limit]	--
select * from [Fact].[Deduction]

--Time
insert into  [Dimension].[Time]([TimeID]
      ,[Date]
      ,[YearID]
      ,[YearDisplay]
      ,[HalfYearID]
      ,[HalfYearDisplay]
      ,[QuarterID]
      ,[QuarterDisplay]
      ,[MonthID]
      ,[MonthDisplay]
      ,[SourceSystemID]
      ,[DateCreated]
      ,[DateExpired]
      ,[DateUpdated]
      ,[CurrentYN]
      ,[IsWeekDay])
select [TimeID]
      ,[Date]
      ,[YearID]
      ,[YearDisplay]
      ,[HalfYearID]
      ,[HalfYearDisplay]
      ,[QuarterID]
      ,[QuarterDisplay]
      ,[MonthID]
      ,[MonthDisplay]
      ,[SourceSystemID]
      ,[DateCreated]
      ,[DateExpired]
      ,[DateUpdated]
      ,[CurrentYN]
      ,[IsWeekDay] from [TARDIS].dbo.[Time]
--Here is the time.
select * from [Dimension].[Time]


--Xref Tables [8]
select * from [dbo].[AssuredXRef]
select * from [dbo].[BranchXRef]
select * from [dbo].[BrokerXRef]
select * from [dbo].[CompanyXRef]
select * from [dbo].[ProductXRef]
select * from [dbo].[SectionTypeXRef]
select * from [dbo].[StatusXRef]
select * from [dbo].[UnderwriterXRef]

--Temp Tables used for Updating
select * from Temp_Policy
select * from Temp_Section
select * from Temp_Coverage
select * from Temp_TransactionType
select * from Temp_RevenueType
select * from Temp_Currency

--Update Commands for all the LOOKUPS

--1. For Dimension Policy
--Update command 
update A
SET A.YearOfAccount=B.YearOfAccount,
	A.MasterReference =B.MasterReference ,
	A.Domicile=B.Domicile,
	A.InceptionDate=B.InceptionDate,
	A.ExpiryDate=B.ExpiryDate
from [Dimension].[Policy] as A join Temp_Policy as B
on A.MasterNumber=B.MasterNumber and A.MasterSeq=B.MasterSeq
go
Truncate Table Temp_Policy
go

--2. For Dimension Policy Section
update A
set A.SectionLongName = B.SectionLongName,
	A.SignedLinePercentage = B.SignedLinePercentage,
	A.SignedOrderPercentage = B.SignedOrderPercentage,
	A.WrittenOrderPercentage = B.WrittenOrderPercentage,
	A.SectionTitle = B.Conv_SectionTitle,
	A.SectionSequence = B.[Conv_Section Seq]
from [Dimension].[PolicySection] as A join Temp_Section as B
on A.SectionKey=B.SectionKey


--3. For Dimension Policy Coverage
Update A
set A.MinDeductible = B.MinDeductible,
	A.MaxDeductible = B.MaxDeductible,
	A.CoverageSequence = B.Conv_CoverageSeq,
	A.CoverageTitle = B.CoverageTitle
 from [Dimension].[PolicyCoverage] as A join Temp_Coverage as B
on A.CoverageKey=B.Conv_CoverageKey
 

--4. For Dimension Transaction Type
Update A
set A.TransactionTypeDescription=B.TransactionTypeDescription
from [Dimension].[TransactionType] as A join Temp_TransactionType as B
on A.TransactionTypeCode=B.TransactionTypeCode

--5. For Dimension Revenue Type
update A 
set A.RevenueTypeName = B.RevenueTypeName,
A.RevenueTypeDescription = B.RevenueTypeDescription
from [Dimension].[RevenueType] as  A join Temp_RevenueType as B
on A.RevenueTypeCode=B.RevenueTypeCode

--6. For Dimension Currency
Update A
set A.CurrDescription =B.CurrDescription
from [Dimension].[Currency] as  A join Temp_Currency as B
on A.CurrCode=B.CurrCode

----------------------------------------------------------------
--1. For Fact Premiun
Update A
set A.BasePremiumAmount=B.BasePremiumAmount,
	A.SignedPremiumAmount=B.SignedPremiumAmount,
	A.PredictedPremiumAmount=B.PredictedPremiumAmount,
	A.WrittenPremiumAmount=B.WrittenPremiumAmount
from fact.Premium as A JOIN Temp_FactPremium as B
on A.SourceIdentifier=B.SourceIdentifier







