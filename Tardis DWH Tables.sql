/*********************************************************************
Author		:Umesh Kumar
Purspose	:Create TARDIS Data Warehouse DB
Date		:Sep 4,2025
Database	:[TARDISDW]
**********************************************************************/
use TARDISDW
go
sp_help'[Dimension].[Policy]'
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

select * from [dbo].[Temp_Policy_newwwwwwwwwwwwwwwww]

--Xref Tables
select * from AssuredXRef
select * from BranchXRef
select * from BrokerXRef
select * from CompanyXRef
select * from ProductXRef
select * from UnderwriterXRef
select * from StatusXRef
select * from SectionTypeXRef

select * from Dimension.RevenueType

select * from Dimension.date

select * from Dimension.Time

--SSIS_Tardis_DWH

select * from [Tardis_Stage].dbo.[SSIS_Logs]	--All Custom loggings
select * from [Tardis_Stage].[dbo].[sysssislog] --Readymade Loaggings

--These are the Insurance Data
--Dimension Table [7]
--1. DimPolicy
select * from [Dimension].[Policy]        -- 379													--done



--2. DimPolicySection
select PolicyID,MasterNumber+MasterSeq as PolicyKey -- Copy this code in SSIS (Lkp)					--done
  from [Dimension].[Policy] 
select * from [Dimension].[PolicySection] -- 365
Insert into [dbo].[SSIS_Logs] values (?,getdate(),?,'SuccessFull','Package Executed Successfully.')
sp_help'[Dimension].[PolicySection]'

--3. DimPolicyCoverage
select * from [Dimension].[PolicyCoverage]	--365														--done			[Work on this later]
sp_help '[Dimension].[PolicyCoverage]'
Insert into [dbo].[SSIS_Logs] values (?,getdate(),?,'SuccessFull','Package Executed Successfully.')

--4. DimTransactionType
select * from [Dimension].[TransactionType]		--1												--done
sp_help'[Dimension].[TransactionType]'

--5. DimRevenueTypee
select * from [Dimension].[RevenueType]		--2														--done
sp_help'[Dimension].[RevenueType]'

--6. DimCurrency
select * from [Dimension].[Currency]	--3													--done
sp_help'[Dimension].[Currency]'


select * from [Dimension].[Time]--Date got loaded from TRADIS db to TARDISDW directly without using stage through SQL code
 
 select *  from  [Dimension].[Date] --This Table you need to use it.

  sp_help'[Dimension].[Time]'
 sp_help'[Dimension].[DATE]'
  
  --7 Dimension Tables
select *  from [Dimension].[Policy]			--379
select *  from [Dimension].[PolicySection]	--365
select *  from [Dimension].[PolicyCoverage] --365
select *  from [Dimension].[TransactionType]--1
select *  from [Dimension].[RevenueType]	--2
select *  from [Dimension].[Currency]		--3
 --------------------------------------		Fact Tables		-----------------------------------
 --3 Fact Tables
 select * from [Fact].[Premium]  
 select * from [Fact].[Limit]
 select * from [Fact].[Deduction]

--1. Fact Premium
select * from [Fact].[Premium]  
sp_help'[Fact].[Premium]'

--2. Fact Limit
select * from [Fact].[Limit]
sp_help'[Fact].[Limit]'

--3. Fact Deduction
select * from [Fact].[Deduction]
sp_help'[Fact].[Deduction]'

--Xref Tables [8]
select * from AssuredXRef
select * from BranchXRef
select * from BrokerXRef
select * from CompanyXRef
select * from ProductXref
select * from SectionTypeXRef
select * from StatusXRef
select * from UnderwriterXRef
 

/*
 delete from [dbo].[AssuredXRef]
 delete from [dbo].[BranchXRef]
 delete from [dbo].[BrokerXRef]
 delete from [dbo].[CompanyXRef]
 delete FROM ProductXref
 delete from [dbo].[SectionTypeXRef]
 delete from [dbo].[StatusXRef]
 delete from [dbo].[UnderwriterXRef]
 */

-- 1. Review duplicates first (safety check)
SELECT ProductCode, COUNT(*) AS DuplicateCount
FROM dbo.ProductXRef
GROUP BY ProductCode
HAVING COUNT(*) > 1;




 -- Update command for SSIS Incremental loading:

select * from [Dimension].[Policy] 

update [Dimension].[Policy]
set  YearOfAccount = ?,  --0
	MasterReference = ?, --1
	Domicile = ?,		 --2
	InceptionDate = ?,   --3
	ExpiryDate = ?		 --4
where MasterNumber=? and MasterSeq =?
				-- 5				6
update [Dimension].[Policy]
set  YearOfAccount = ?,
	MasterReference = ?,
	Domicile = ?,
	InceptionDate = ?,
	ExpiryDate = ?
where MasterNumber=? and MasterSeq =?

---------------------------------------------------------------------------------------------------------------------------------------
---------------------------------------------------------------------------------------------------------------------------------------
 -- The following is for [To know the seperated unmatched data]

 --Union all or Merge

 --		status	Statuscode		StatusDesc  Createdate		updatedate CurrentYN
 --		-1			un			 unmatched 

 USE TARDISDW_Lookup
 go

 select * from TARDISDW_Lookup.DBO.StatusXRef
 SELECT * FROM StatusXRef

 -- Allow explicit inserts into identity column
SET IDENTITY_INSERT TARDISDW_Lookup.DBO.StatusXRef ON;

INSERT INTO TARDISDW_Lookup.DBO.StatusXRef (StatusID, StatusCode, StatusDesc, CreateDate, UpdatedDate, CurrentYN)
VALUES (-1, 'UN', 'Un Matched', GETDATE(), GETDATE(), 1);

-- Turn identity insert back off
SET IDENTITY_INSERT TARDISDW_Lookup.DBO.StatusXRef OFF;

------------------------------------------------------		end		---------------------------------------------------------------


--Insert the unmatched data
select * from StatusXRef

--insert data in xref
set identity_insert StatusXRef on;

--Try to insert an explicit ID value of -1 in StatusXRef table
insert into StatusXRef(StatusID,StatusCode,StatusDesc) values(-1,'UN','Un Matched')

set identity_insert StatusXRef off;

--For Assured id

set identity_insert AssuredXRef on;
insert into AssuredXRef(AssuredID,AssuredCode,AssuredName) values(-1,'UN','Unmatched')
set identity_insert AssuredXRef off;

select * from AssuredXRef

--For BrokerXRef

set identity_insert BrokerXRef on;
insert into BrokerXRef(BrokerID,BrokerCode,BrokerName) values(-1,'UN','Unmatched')
set identity_insert BrokerXRef off;

select * from BrokerXRef
 
--For BranchXRef
set identity_insert BranchXRef on;
insert into BranchXRef(BranchID,BranchCode,BranchName)values(-1,'UN','Unmatched')
set identity_insert BranchXRef off

--For BranchXRef
set identity_insert CompanyXRef on;
insert into CompanyXRef(CompanyID,CompanyCode,CompanyName) values(-1,'UN','Unmatched')
set identity_insert CompanyXRef off;

 --For ProductXRef
 set identity_insert ProductXRef on;
 insert into ProductXRef(ProductID,ProductCode,ProductName) values(-1,'UN','Unmatched')
 set identity_insert ProductXRef off;

 --For UnderwriterXRef
 set identity_insert UnderwriterXRef on;
 insert into UnderwriterXRef(UnderwriterID,UnderwriterCode,UnderwriterName) values(-1,'UN','Unmatched')
 set identity_insert UnderwriterXRef off;

