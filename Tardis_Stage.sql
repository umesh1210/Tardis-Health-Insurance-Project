/*********************************************************************
Author		:Umesh Kumar
Purspose	:Create Stage DB
Date		:Sep 3,2025
Database	:[Tardis_Stage]
**********************************************************************/


--	OLAP Database :
-- Dest_Tardis_Stage
-- Source_TARDIS

use [Tardis_Stage]
go 

select * from [dbo].[SSIS_Logs]

-- These are the 4 tables from OLTP db:-
select * from Stage_Policy		--1000 Rows
select * from Stage_RevenueType	--2 Rows
select * from Stage_Currency		--3 Rows
select * from Stage_TransactionType	--1 Rows

/* For truncate table
 truncate table Stage_Policy
 truncate table Stage_RevenueType
 truncate table Stage_Currency
 truncate table Stage_TransactionType
 */


 -- These are from 5 EXCEL sheets:-
 select * from  Stage_PolicySection	  -- 1003 rows
 select * from  Stage_PolicyCoverage  -- 1003 rows    
  select * from Stage_Premium		  -- 1001 rows
  select * from Stage_Limit			  -- 1000 rows
  select * from Stage_Deduction 	  -- 1000 rows

  
/* For truncate table
 truncate table Stage_PolicySection
 truncate table Stage_PolicyCoverage
 truncate table Stage_Premium
 truncate table Stage_Limit
 truncate table Stage_Deduction
 */

 -- Creating a Logging Table:
use [Tardis_Stage]
go

create table SSIS_Logs(
ID				int				primary key identity(1,1),
PkgName			varchar(150)	not null,
PkgExecTime		datetime		not null,
Row_Count		int				not null,
PkgExecStatus	varchar(100)	not null,
PkgExecMsg		varchar(150)	not null
)

select * from SSIS_Logs

-- Success Log				  0			  1
insert into SSIS_Logs values (?,getdate(),?,'SuccessFull','Package Executed Successfully.')

-- Failure Log 
select * from sysssislog   -- This log i made in the SSIS by [Windows Event Logv ]

--000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000 
--000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000 


select * from SSIS_Logs
select * from [dbo].[Stage_Deduction]		--1000 Rows
select * from [dbo].[Stage_Limit]			--1000 Rows
select * from [dbo].[Stage_Policy]			--1000 Rows
select * from [dbo].[Stage_PolicyCoverage]	--1003 Rows
select * from [dbo].[Stage_PolicySection]	--1003 Rows
select * from [dbo].[Stage_Premium]			--1001 Rows
select * from [dbo].[Stage_Currency]		--3 Rows
select * from [dbo].[Stage_RevenueType]		--2 Rows
select * from [dbo].[Stage_TransactionType] --1 Rows

-- Dest_Tardis_Stage
-- Source_TARDIS


use [TARDIS]
go

select * from Deduction
select * from Limit
select * from Policy
select * from PolicyCoverage
select * from PolicySection
select * from Premium
select * from Finance.Currency
select * from RevenueType
select * from TransactionType
 
--	OLTP Database : 

use tardis
go

select * from policy		--1000 Rows
select * from RevenueType	--2 Rows
select * from Finance.Currency --3 Rows
select * from TransactionType  -- 1 Rows

-- Excel :

PolicySection
PolicyCoverage
Premium
Limit
Deduction


select * from PolicySection

----------------------------------------------     END      -----------------------------------------------------------------------












--------------------
--Rough

use [TARDIS]
go

select * from [dbo].[Policy]



use [Tardis_Stage]
go

select * from [dbo].[New_Tardis_Transaction_Type]
select * from [dbo].[New_Tardis_Policy]


use [TARDISDW]
go
drop database [Tardis_Stage]

select * from [Dimension].[TransactionType]
select * from [Dimension].[Policy]


 
update [Dimension].[Policy]
set		YearOfAccount=?,
		MasterReference =?,
		Domicile=?,
		InceptionDate =?,
		ExpiryDate=?
where MasterNumber=? and  MasterSeq =?
 
 
 
 
 select * from [dbo].[Temp Table]