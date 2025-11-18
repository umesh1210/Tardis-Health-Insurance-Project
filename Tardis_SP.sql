/*********************************************************************
Author		:Umesh Kumar
Purspose	:Performing Incremental Loading Using Stored Procedures in SQL
Date		:Sep 15,2025
Database	:[TARDISDW_SP]
**********************************************************************/
USE TARDISDW_SP
GO
--Insert the Xref 8 tables:
select * from [dbo].[AssuredXRef]
select * from [dbo].[BranchXRef]
select * from [dbo].[BrokerXRef]
select * from [dbo].[CompanyXRef]
select * from [dbo].[ProductXRef]
select * from [dbo].[SectionTypeXRef]
select * from [dbo].[StatusXRef]
select * from [dbo].[UnderwriterXRef]

-- Load NewTimeDimScripts - Dim_Date
select * from Dimension.date


select * from Dimension.Policy--379						These are the date should have the {original dates} not the present/current date
select * from Dimension.Currency--3
select * from Dimension.PolicyCoverage --371
select * from Dimension.PolicySection --371
select * from Dimension.RevenueType --2
select * from Dimension.TransactionType --1

select * from Fact.Premium		--371
select * from fact.Limit		--370
select * from fact.Deduction	--370



--Dimension Tables from SCD
select *  from [TARDISDW_SCD].[Dimension].[Policy]			--379  Pastdate
select *  from [TARDISDW_SCD].[Dimension].[PolicySection]	--365  Present date
select *  from [TARDISDW_SCD].[Dimension].[PolicyCoverage] --365  Present date
select *  from [TARDISDW_SCD].[Dimension].[TransactionType]--1	   Past date
select *  from [TARDISDW_SCD].[Dimension].[RevenueType]	--2	   Past date
select *  from [TARDISDW_SCD].[Dimension].[Currency]		--3	   Past date



--Comparing SP to SCD  {Final Result should be}
select * from Tardis_Stage.dbo.Stage_Policy         --Stage
select *  from [TARDISDW_SCD].[Dimension].[Policy]	--SCD		--379  Pastdate					Bug in SCD          ClassID should be 1 defult
select * from Dimension.Policy--379					--SP

select * from Tardis_Stage.dbo.Stage_PolicySection
select *  from [TARDISDW_SCD].[Dimension].[PolicySection]	--365  Present date
select * from Dimension.PolicySection --371

select *  from [TARDISDW_SCD].[Dimension].[PolicyCoverage] --365  Present date
select * from Dimension.PolicyCoverage --371

select *  from [TARDISDW_SCD].[Dimension].[TransactionType]--1	   Past date
select * from Dimension.TransactionType --1

select *  from [TARDISDW_SCD].[Dimension].[RevenueType]	--2	   Past date
select * from Dimension.RevenueType --2

select *  from [TARDISDW_SCD].[Dimension].[Currency]		--3	   Past date
select * from Dimension.Currency--3
 





















select * from Tardis_Stage.dbo.Stage_Currency --Source
 
 
--_____________________________________________________________________________________________________________________________________________
--_____________________________________________________________________________________________________________________________________________
--											Dimension Tables

--1. DimPolicy

--select * from TARDISDW_SP.Dimension.Policy
--select * from Tardis_Stage.dbo.Stage_Policy


create proc ETL_Load_DimPolicy_Incr_Loading
as 
begin
--For Insert
insert into TARDISDW_SP.Dimension.Policy
		(MasterNumber,MasterSeq,StatusID,ProductID,YearOfAccount,MasterReference,AssuredID,BrokerID,DepartmentID,BranchID,
		AreaID,Domicile,ClassID,CompanyID,InceptionDate,ExpiryDate,UnderwriterID,MethodOfAcceptanceID,
		RenewalStatusID,RenewalStatusCode,DateCreated,DateExpired,DateUpdated,CurrentYN,SourceSystemID)
select S.MasterNumber,S.MasterSeq,SXR.StatusID,PXR.ProductID,S.YearOfAccount,S.MasterReference,AXR.AssuredID,BROXR.BrokerID,1 as DepartmentID,BRAXR.BranchID,
		1 as AreaID,S.Domicile,1 as ClassID,CXR.CompanyID,S.InceptionDate,S.ExpiryDate,UXR.UnderwriterID,1 as MethodOfAcceptanceID,
		1 as RenewalStatusID,S.RenewalStatusCode,S.DateCreated,S.DateExpired,S.DateUpdated,S.CurrentYN,S.SourceSystemID
		
		--SXR.StatusID,
		--PXR.ProductID,
		--AXR.AssuredID,
		--BROXR.BrokerID,
		--1 as DepartmentID,
		--BRAXR.BranchID,
		--CXR.CompanyID,
		--UXR.UnderwriterID,
		--1 as MethodOfAcceptanceID,
		--1 as RenewalStatusID,
		--1 as ClassID,
		--1 as AreaID
from Tardis_Stage.dbo.Stage_Policy as S join TARDISDW_SP.Dbo.StatusXRef as SXR 
on S.MasterStatusCode=SXR.StatusCode join TARDISDW_SP.Dbo.ProductXRef as PXR 
on s.MasterProductCode=PXR.ProductCode join TARDISDW_SP.Dbo.AssuredXRef as AXR
on s.AssuredNameCode=AXR.AssuredCode join TARDISDW_SP.Dbo.BrokerXRef as BROXR
on S.BrokerNameCode=BROXR.BrokerCode join TARDISDW_SP.Dbo.BranchXRef as BRAXR
on S.BranchName=BRAXR.BranchCode join TARDISDW_SP.Dbo.CompanyXRef as CXR
on S.CompanyName=CXR.CompanyCode JOIN TARDISDW_SP.Dbo.UnderwriterXRef as UXR
on S.UnderwriterNameCode=UXR.UnderwriterCode left join TARDISDW_SP.Dimension.Policy as D 
ON S.MasterNumber=D.MasterNumber and S.MasterSeq=d.MasterSeq
--where D.PolicyID is null

-- For Update
update D
set D.YearOfAccount=S.YearOfAccount,D.MasterReference=S.MasterReference,D.Domicile=S.Domicile,D.InceptionDate=S.InceptionDate,D.ExpiryDate=S.ExpiryDate
from TARDISDW_SP.Dimension.Policy as D join Tardis_Stage.dbo.Stage_Policy as S 
on S.MasterNumber=D.MasterNumber and S.MasterSeq=D.MasterSeq
where D.YearOfAccount<>S.YearOfAccount OR D.MasterReference<>S.MasterReference OR D.Domicile<>S.Domicile OR D.InceptionDate<>S.InceptionDate OR D.ExpiryDate<>S.ExpiryDate

end
go

exec ETL_Load_DimPolicy_Incr_Loading

DROP PROC ETL_Load_DimPolicy_Incr_Loading

  --[[		Testing Purpose		]]

 -- Insert Policy
Insert into Tardis_Stage.dbo.Stage_Policy ( MasterNumber, MasterSeq, MasterTitle, MasterStatusCode, StatusID, MasterProductCode, ProductID,
YearOfAccount,MasterReference,AssuredNameCode, AssuredID, BrokerNameCode, BrokerID, DepartmentName,DepartmentID,
BranchID,BranchName, AreaID, Domicile, ClassCode, ClassID, CompanyID, CompanyName, UnderwriterNameCode, UnderwriterID, 
RenewalStatusID, RenewalStatusCode)
Values('003MGF', '001', 'Test lookup Increament', 'M5', 36, 'LIBCMM', 35, 2020, '64932C140ALI', 'TORUSNTL', '40420',
'0801', '32399', 'REI', 18, 30, 'NJNT', 363, 'India', 'LIAB', 9, 6, 'TORUSNTL', 'DOBR0003', '164495', 12, 'NR')

-- Update Policy
/*
select MasterTitle, Domicile, * from Tardis_Stage.dbo.Stage_Policy
where MasterNumber='Q05WNT' and MasterSeq='001'
*/
Update Tardis_Stage.dbo.Stage_Policy 
Set MasterTitle = 'Incremental Update Testing', Domicile = 'India'
where MasterNumber='Q05WNT' and MasterSeq='001'

SELECT DateCreated,DateExpired,DateUpdated,CurrentYN,SourceSystemID FROM TARDISDW_SP.Dimension.Policy 
--	DateCreated						DateExpired					DateUpdated				CurrentYN	SourceSystemID
--2025-09-18 08:02:48.287		9998-12-31 00:00:00.000		2025-09-18 08:02:48.287			1			5


--_____________________________________________________________________________________________________________________________________________
--_____________________________________________________________________________________________________________________________________________
--2.DimPolicySection

select * from Tardis_Stage.dbo.Stage_PolicySection
select * from TARDISDW_SP.Dimension.PolicySection

create proc ETL_Load_DimPolicySection_Incr_Loading
as
begin

 --For Insert 
;WITH PolicyKeyMap AS (
    SELECT 
        PolicyID,
        CAST(MasterNumber AS varchar(20)) + CAST(MasterSeq AS varchar(20)) AS PolicyKey
    FROM TARDISDW_SP.Dimension.Policy
)

insert into TARDISDW_SP.Dimension.PolicySection
			(SectionKey,SectionLongName,SignedLinePercentage,SignedOrderPercentage,WrittenOrderPercentage,SectionSequence,
			SectionTypeID,SectionSubTypeID,SectionTitle,PolicyID,DateCreated,DateExpired,DateUpdated,CurrentYN,SourceSystemID)

select  --PolicyID ,
		--SectionTypeID,
		--1 as SectionSubTypeID
		S.SectionKey,
		S.SectionLongName,
		S.SignedLinePercentage,
		S.SignedOrderPercentage,
		S.WrittenOrderPercentage,
		s.[Section Seq] as SectionSequence,--------------
		STXR.SectionTypeID,
		1 as SectionSubTypeID,
		s.SectionTitle,
		D.PolicyID,
		STXR.DateCreated,STXR.DateExpired,STXR.DateUpdated,STXR.CurrentYN,SourceSystemID

		from Tardis_Stage.dbo.Stage_PolicySection S join PolicyKeyMap D 
		on S.PolicyKey=d.PolicyKey join TARDISDW_SP.dbo.SectionTypeXRef as STXR
		on S.SectionTypeCode=STXR.SectionTypeCode
		LEFT JOIN TARDISDW_SP.Dimension.PolicySection Dim
		on s.SectionKey=Dim.SectionKey
		where dim.SectionKey is null
		 
		
-- For Update 
update D
set D.SectionLongName=s.SectionLongName,d.SignedLinePercentage=s.SignedLinePercentage,D.SignedOrderPercentage=S.SignedOrderPercentage,D.WrittenOrderPercentage
=S.WrittenOrderPercentage,D.SectionTitle=S.SectionTitle,D.SectionSequence=S.[Section Seq]
from TARDISDW_SP.Dimension.PolicySection D join Tardis_Stage.dbo.Stage_PolicySection S on d.SectionKey=s.SectionKey
where D.SectionLongName<>s.SectionLongName or d.SignedLinePercentage<>s.SignedLinePercentage or D.SignedOrderPercentage<>S.SignedOrderPercentage or D.WrittenOrderPercentage
<>S.WrittenOrderPercentage or D.SectionTitle<>S.SectionTitle or D.SectionSequence<>S.[Section Seq]

end
go

EXEC ETL_Load_DimPolicySection_Incr_Loading

--_____________________________________________________________________________________________________________________________________________
--_____________________________________________________________________________________________________________________________________________

--3.DimPolicyCoverage


select * from Tardis_Stage.dbo.Stage_PolicyCoverage
select * from TARDISDW_SP.Dimension.PolicyCoverage

create proc ETL_Load_DimPolicyCoverage_Incr_Loading
as 
begin
--For Insert 
insert into TARDISDW_SP.Dimension.PolicyCoverage
	(CoverageKey,PolicySectionID,CoverageTypeID,MinDeductible,MaxDeductible,CoverageSequence,
	CoverageTitle,DateCreated,DateExpired,DateUpdated,CurrentYN,SourceSystemID)

select  --PolicySectionID,
		--1 as CoverageTypeID
		S.CoverageKey,
		D.PolicySectionID,
		1 as CoverageTypeID,
		S.MinDeductible,
		S.MaxDeductible,
		CoverageSeq as CoverageSequence,
		S.CoverageTitle,
		D.DateCreated,
		D.DateExpired,
		D.DateUpdated,
		D.CurrentYN,
		D.SourceSystemID
from Tardis_Stage.dbo.Stage_PolicyCoverage S  join TARDISDW_SP.Dimension.PolicySection D on S.SectionKey=D.SectionKey
Left join TARDISDW_SP.Dimension.PolicyCoverage as dest
on S.CoverageKey=dest.CoverageKey
where Dest.CoverageKey is null

--For Update
update D
set D.MinDeductible=S.MinDeductible,D.MaxDeductible=S.MaxDeductible,D.CoverageSequence=S.CoverageSeq,D.CoverageTitle=S.CoverageTitle
from Tardis_Stage.dbo.Stage_PolicyCoverage S join TARDISDW_SP.Dimension.PolicyCoverage as D
on s.CoverageKey=d.CoverageKey
WHERE D.MinDeductible<>S.MinDeductible or D.MaxDeductible<>S.MaxDeductible or D.CoverageSequence<>S.CoverageSeq or D.CoverageTitle<>S.CoverageTitle
end

exec ETL_Load_DimPolicyCoverage_Incr_Loading
--_____________________________________________________________________________________________________________________________________________
--_____________________________________________________________________________________________________________________________________________

 --4. DimTransactionType

--select * from Tardis_Stage.dbo.Stage_TransactionType
--select * from TARDISDW_SP.Dimension.TransactionType

 create proc ETL_Load_DimTransactionType_Incr_Loading
 as
 begin
 --For Insert 
 insert into  TARDISDW_SP.Dimension.TransactionType(TransactionTypeCode,TransactionTypeDescription,DateCreated,DateExpired,DateUpdated,CurrentYN,SourceSystemID )
 select S.TransactionTypeCode,
		S.TransactionTypeDescription,
		S.DateCreated,
		S.DateExpired,
		S.DateUpdated,
		S.CurrentYN,
		S.SourceSystemID 
 from Tardis_Stage.dbo.Stage_TransactionType as S left join TARDISDW_SP.Dimension.TransactionType as D
 on S.TransactionTypeCode=D.TransactionTypeCode
 where d.TransactionTypeCode is null

 --For Update
 update d
 set d.TransactionTypeDescription=S.TransactionTypeDescription
 from Tardis_Stage.dbo.Stage_TransactionType as S join TARDISDW_SP.Dimension.TransactionType as D 
 on s.TransactionTypeCode=d.TransactionTypeCode
 where d.TransactionTypeDescription<>S.TransactionTypeDescription
 end
 go

 --Run
 exec   ETL_Load_DimTransactionType_Incr_Loading


 --[[		Testing Purpose		]]
   --Insert TransactionType
Insert into Tardis_Stage.dbo.Stage_TransactionType(TransactionTypeCode, TransactionTypeDescription)
Values (222, 'TestingLookup')


--Update TransactionType
select * from Tardis_Stage.dbo.Stage_TransactionType
where TransactionTypeID=2

Update Tardis_Stage.dbo.Stage_TransactionType
Set TransactionTypeDescription = 'Hookup Testing'
where TransactionTypeID=1
 --_____________________________________________________________________________________________________________________________________________
--_____________________________________________________________________________________________________________________________________________

 --5. DimRevenue

 CREATE PROC ETL_Load_DimRevenueType_Incr_Loading
 as
 begin
 --For Insert 
 insert into TARDISDW_SP.Dimension.RevenueType
 (RevenueTypeCode,RevenueTypeName,RevenueTypeDescription,DateCreated,DateExpired,DateUpdated,CurrentYN,SourceSystemID)
 select s.RevenueTypeCode,
		s.RevenueTypeName,
		s.RevenueTypeDescription,
		s.DateCreated,
		s.DateExpired,
		s.DateUpdated,
		s.CurrentYN,
		s.SourceSystemID
		from Tardis_Stage.dbo.Stage_RevenueType as S
 left join TARDISDW_SP.Dimension.RevenueType as D 
 on D.RevenueTypeCode=s.RevenueTypeCode
 where D.RevenueTypeCode is null

 -- For Update
 update D
 set D.RevenueTypeName=S.RevenueTypeName,D.RevenueTypeDescription=S.RevenueTypeDescription
from TARDISDW_SP.Dimension.RevenueType D JOIN Tardis_Stage.dbo.Stage_RevenueType as S
 on D.RevenueTypeCode=s.RevenueTypeCode
where D.RevenueTypeName<>S.RevenueTypeName or D.RevenueTypeDescription<>S.RevenueTypeDescription
end
go

exec ETL_Load_DimRevenueType_Incr_Loading

-- [[		Testing Purpose		]]
 select * from Tardis_Stage.dbo.Stage_RevenueType
 select * from TARDISDW_SP.Dimension.RevenueType
 
--Insert RevenueType
Insert into Tardis_Stage.dbo.Stage_RevenueType (RevenueTypeCode, RevenueTypeName, RevenueTypeDescription, Deposit)
Values ('MBR', 'gg', 'Delivery Mgr', 3)

/*
--Update RevenueType
Select * from [dbo].[RevenueType]
where RevenueTypeID = 36
*/
Update Tardis_Stage.dbo.Stage_RevenueType
Set RevenueTypeDescription = 'Sales mngr'
where RevenueTypeID =36

--_____________________________________________________________________________________________________________________________________________
--_____________________________________________________________________________________________________________________________________________

--6. DimCurrency  
 create proc ETL_Load_DimCurrency_Incr_Loading
 as
 begin

--For Insert
INSERT INTO TARDISDW_SP.Dimension.Currency
    (CurrCode, CurrDescription, CreateDate, UpdateDate, BatchID, SourceSystemID, CurrentYN)
SELECT 
    S.CurrCode, 
    S.CurrDescription, 
    S.CreateDate, 
    S.UpdateDate, 
    S.BatchID, 
    S.SourceSystemID, 
    S.CurrentYN
FROM Tardis_Stage.dbo.Stage_Currency AS S
LEFT JOIN TARDISDW_SP.Dimension.Currency AS D
    ON D.CurrCode = S.CurrCode  -- Business Key
WHERE D.CurrCode IS NULL;

--For Update
UPDATE D
SET 
    D.CurrDescription = S.CurrDescription
  FROM TARDISDW_SP.Dimension.Currency AS D
INNER JOIN Tardis_Stage.dbo.Stage_Currency AS S
    ON D.CurrCode = S.CurrCode  -- Business Key
WHERE 
    D.CurrDescription <> S.CurrDescription
;
end
go

 exec ETL_Load_DimCurrency_Incr_Loading
 

 --[[		Testing Purpose		]]
 select * from Tardis_Stage.dbo.Stage_Currency
 select * from TARDISDW_SP.Dimension.Currency
 
 --Insert Currency
Insert into Tardis_Stage.dbo.Stage_Currency (CurrentYN,SourceSystemID,BatchID,CurrCode,CurrDescription,CurrencyID,CreateDate,UpdateDate,ExpiredDate
)
Values (1,-1,-1,'AUD', 'AustralianDollar',4,getdate(),getdate(),'9998-12-31 00:00:00.000')

--Update Currency
Update Tardis_Stage.dbo.Stage_Currency
Set CurrDescription = 'IndianCurrency'
Where CurrCode = 'INR'

/*
--Update [Finance].[Currency]
select * from Tardis_Stage.dbo.Stage_Currency
Where CurrCode = 'INR'*/
--_____________________________________________________________________________________________________________________________________________
--_____________________________________________________________________________________________________________________________________________
--_____________________________________________________________________________________________________________________________________________
--_____________________________________________________________________________________________________________________________________________
--_____________________________________________________________________________________________________________________________________________
--_____________________________________________________________________________________________________________________________________________
--_____________________________________________________________________________________________________________________________________________
--_____________________________________________________________________________________________________________________________________________
--_____________________________________________________________________________________________________________________________________________
--_____________________________________________________________________________________________________________________________________________

 
--											Fact Tables
  

 --1. Fact Premimum

 select * from Tardis_Stage.dbo.Stage_Premium
 select * from TARDISDW_SP.Fact.Premium

 create proc ETL_Load_FactPremium_Incr_Loading
 as 
 begin
 --For Insert
 insert into TARDISDW_SP.Fact.Premium(SourceIdentifier,TransactionTypeID,RevenueTypeID,OriginalCurrencyID,SettlementCurrenyID,PolicyCoverageID,
 TimeID,BasePremiumAmount,SignedPremiumAmount,PredictedPremiumAmount,WrittenPremiumAmount,SourceSystemID,DateCreated,DateUpdated,CurrentYN)

 select --PolicyCoverageID,
		--TransactionTypeID,
		--RevenueTypeID,
		--C.CurrencyID as SettlementCurrenyID,
		--CC.CurrencyID as OriginalCurrencyID,
		--TimeID
		S.SourceIdentifier,T.TransactionTypeID,R.RevenueTypeID,CC.CurrencyID as OriginalCurrencyID,C.CurrencyID as SettlementCurrenyID,P.PolicyCoverageID,
 DA.TimeID,S.BasePremiumAmount,S.SignedPremiumAmount,S.PredictedPremiumAmount,S.WrittenPremiumAmount,T.SourceSystemID,T.DateCreated,P.DateUpdated,P.CurrentYN
 
 from Tardis_Stage.dbo.Stage_Premium S join TARDISDW_SP.Dimension.PolicyCoverage P 
 on S.CoverageKey=p.CoverageKey join TARDISDW_SP.Dimension.TransactionType T 
 on T.TransactionTypeCode=S.TransactionCode join TARDISDW_SP.Dimension.RevenueType R
 on R.RevenueTypeCode=S.RevenueType join TARDISDW_SP.Dimension.Currency C
 on C.CurrCode=s.CurrCode join TARDISDW_SP.Dimension.Currency CC 
 on CC.CurrCode=S.CurrCode join TARDISDW_SP.Dimension.Date Da
 on Da.Date=s.Date Left join TARDISDW_SP.Fact.Premium as Dest
 on Dest.SourceIdentifier=S.SourceIdentifier
 where Dest.SourceIdentifier is null;

 --For Update
 update D
 set D.BasePremiumAmount=S.BasePremiumAmount,D.SignedPremiumAmount=S.SignedPremiumAmount,D.PredictedPremiumAmount
=S.PredictedPremiumAmount,D.WrittenPremiumAmount=S.WrittenPremiumAmount
 from Tardis_Stage.dbo.Stage_Premium S join TARDISDW_SP.Fact.Premium as D
 on D.SourceIdentifier=S.SourceIdentifier
 where D.BasePremiumAmount<>S.BasePremiumAmount or D.SignedPremiumAmount<>S.SignedPremiumAmount or D.PredictedPremiumAmount
<>S.PredictedPremiumAmount or D.WrittenPremiumAmount<>S.WrittenPremiumAmount
end
go

exec ETL_Load_FactPremium_Incr_Loading

 
--_____________________________________________________________________________________________________________________________________________
--_____________________________________________________________________________________________________________________________________________
--2. Fact Limit 

 select * from Tardis_Stage.dbo.Stage_Limit
  select * from TARDISDW_SP.Fact.Limit

  sp_help'TARDISDW_SP.Fact.Limit'

create proc ETL_Load_FactLimit_Incr_Loading
as
begin
-- For Insert 
 insert into TARDISDW_SP.Fact.Limit 
			(SourceIdentifier,TransactionTypeID,OriginalCurrencyID,SettlementCurrencyID,PolicyCoverageID,TimeID,FXRate,
			LimitAmount,SourceSystemID,DateCreated,DateUpdated,CurrentYN)

 select --TransactionTypeID,
		--C.CurrencyID as OriginalCurrencyID,
		--CC.CurrencyID as SettlementCurrencyID,
		--PolicyCoverageID
		--D.TimeID

		S.SourceIdentifier,T.TransactionTypeID,C.CurrencyID as OriginalCurrencyID,CC.CurrencyID as SettlementCurrencyID,P.PolicyCoverageID,D.TimeID,S.FXRate,
			S.LimitAmount,T.SourceSystemID,T.DateCreated,T.DateUpdated,T.CurrentYN


 from Tardis_Stage.dbo.Stage_Limit S join TARDISDW_SP.Dimension.TransactionType T
 on T.TransactionTypeCode=S.TransactionCode join TARDISDW_SP.Dimension.Currency C
 on C.CurrCode=S.CurrCode join TARDISDW_SP.Dimension.Currency CC
 on CC.CurrCode=S.CurrCode join TARDISDW_SP.Dimension.PolicyCoverage P
 on P.CoverageKey=S.CoverageKey join TARDISDW_SP.Dimension.Date D
 on D.Date=S.Date Left JOIN TARDISDW_SP.Fact.Limit L
 on L.SourceIdentifier=S.SourceIdentifier
 where L.SourceIdentifier is null

 --For Update
 update D
 set D.FXRate=S.FXRate,D.LimitAmount=S.LimitAmount
 from Tardis_Stage.dbo.Stage_Limit S join TARDISDW_SP.Fact.Limit D
 on S.SourceIdentifier=D.SourceIdentifier
 WHERE D.FXRate<>S.FXRate or D.LimitAmount<>S.LimitAmount

 end
 go

 exec ETL_Load_FactLimit_Incr_Loading


-- 3. Fact Deduction

CREATE PROC ETL_Load_FactDeduction_Incr_Loading
as 
begin

--For Insert
insert into TARDISDW_SP.Fact.Deduction(SourceIdentifier,TransactionTypeID,RevenueTypeID,PolicyCoverageID,CurrencyID,TimeID,DeductionAmount,FXRate,
DeductionPercentage,SourceSystemID,DateCreated,DateUpdated,CurrentYN)

select  --TransactionTypeID ,
		--RevenueTypeID,
		--PolicyCoverageID,
		--CurrencyID,
		--TimeID
		S.SourceIdentifier,T.TransactionTypeID,R.RevenueTypeID,P.PolicyCoverageID,C.CurrencyID,D.TimeID,S.DeductionAmount,S.FXRate,
s.Percentgage as  DeductionPercentage,T.SourceSystemID,T.DateCreated,T.DateUpdated,T.CurrentYN

from Tardis_Stage.dbo.Stage_Deduction  S join TARDISDW_SP.Dimension.TransactionType as T
on T.TransactionTypeCode=S.TransactionCode join TARDISDW_SP.Dimension.RevenueType as R
on R.RevenueTypeCode=S.RevenueTypeCode join TARDISDW_SP.Dimension.PolicyCoverage as P
on P.CoverageKey =S.CoverageKey join TARDISDW_SP.Dimension.Currency as C
on C.CurrCode=s.CurrCode join TARDISDW_SP.Dimension.Date D
on D.Date=S.Date left join TARDISDW_SP.Fact.Deduction as Deduc
on S.SourceIdentifier=Deduc.SourceIdentifier
where Deduc.SourceIdentifier is null

--For Update
Update D
set D.DeductionAmount=S.DeductionAmount,D.FXRate=S.FXRate,D.DeductionPercentage=S.Percentgage
from Tardis_Stage.dbo.Stage_Deduction  S  join TARDISDW_SP.Fact.Deduction D
on S.SourceIdentifier=D.SourceIdentifier
where  D.DeductionAmount<>S.DeductionAmount OR D.FXRate<>S.FXRate OR D.DeductionPercentage<>S.Percentgage

end


exec ETL_Load_FactDeduction_Incr_Loading



select * from Tardis_Stage.dbo.Stage_Deduction
select * from TARDISDW_SP.Fact.Deduction

------------------------------------------------------END---------------------------------------------------------------------

