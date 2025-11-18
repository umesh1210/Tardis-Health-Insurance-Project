/*********************************************************************
Author		:Umesh
Purspose	:Performing Incremental Loading Using Merge Statement in SQL
Date		:Sep 17,2025
Database	:[TardisDW_Merge]
**********************************************************************/
use TardisDW_Merge
go

select * from TARDISDW_Merge.Dimension.Policy		--379
select * from TARDISDW_Merge.Dimension.PolicySection --371
select * from TARDISDW_Merge.Dimension.PolicyCoverage --371
select * from TARDISDW_Merge.Dimension.TransactionType--1
select * from TARDISDW_Merge.Dimension.RevenueType	--2
select * from TARDISDW_Merge.Dimension.Currency		--3


select * from TARDISDW_Merge.Dimension.Date

select * from TARDISDW_Merge.Fact.Premium	--371
select * from TARDISDW_Merge.Fact.Limit		--370
select * from TARDISDW_Merge.Fact.Deduction	--370


 --Xref Tables [8]
select * from AssuredXRef
select * from BranchXRef
select * from BrokerXRef
select * from CompanyXRef
select * from ProductXref
select * from SectionTypeXRef
select * from StatusXRef
select * from UnderwriterXRef
 
---------------------------------------------Dimension Tables--------------------------------------------------------------------

use TardisDW_Merge
go

-- 1. DimPolicy
create proc ETL_Load_DimPolicy_Merge_Incr_Loading
as
begin
if exists (select * from sys.tables where name ='#temp_Policy')
drop table #temp_Policy

select S.MasterNumber,S.MasterSeq,SXR.StatusID,PXR.ProductID,S.YearOfAccount,S.MasterReference,AXR.AssuredID,BROXR.BrokerID,1 as DepartmentID,BRAXR.BranchID,
		1 as AreaID,S.Domicile,1 as ClassID,CXR.CompanyID,S.InceptionDate,S.ExpiryDate,UXR.UnderwriterID,1 as MethodOfAcceptanceID,
		1 as RenewalStatusID,S.RenewalStatusCode,S.DateCreated,S.DateExpired,S.DateUpdated,S.CurrentYN,S.SourceSystemID into #temp_Policy
from Tardis_Stage.dbo.Stage_Policy as S join TARDISDW_Merge.Dbo.StatusXRef as SXR 
on S.MasterStatusCode=SXR.StatusCode join TARDISDW_Merge.Dbo.ProductXRef as PXR 
on s.MasterProductCode=PXR.ProductCode join TARDISDW_Merge.Dbo.AssuredXRef as AXR
on s.AssuredNameCode=AXR.AssuredCode join TARDISDW_Merge.Dbo.BrokerXRef as BROXR
on S.BrokerNameCode=BROXR.BrokerCode join TARDISDW_Merge.Dbo.BranchXRef as BRAXR
on S.BranchName=BRAXR.BranchCode join TARDISDW_Merge.Dbo.CompanyXRef as CXR
on S.CompanyName=CXR.CompanyCode JOIN TARDISDW_Merge.Dbo.UnderwriterXRef as UXR
on S.UnderwriterNameCode=UXR.UnderwriterCode left join TARDISDW_Merge.Dimension.Policy as D 
ON S.MasterNumber=D.MasterNumber and S.MasterSeq=d.MasterSeq
--select * from #temp_Policy
select *  from [TARDISDW_SCD].[Dimension].[Policy]


merge TARDISDW_Merge.Dimension.Policy as D
using #temp_Policy as S
on S.MasterNumber=D.MasterNumber and S.MasterSeq=D.MasterSeq

--Insert 
when not matched by target
then 
insert([MasterNumber]
      ,[MasterSeq]
      ,[StatusID]
      ,[ProductID]
      ,[YearOfAccount]
      ,[MasterReference]
      ,[AssuredID]
      ,[BrokerID]
      ,[DepartmentID]
      ,[BranchID]
      ,[AreaID]
      ,[Domicile]
      ,[ClassID]
      ,[CompanyID]
      ,[InceptionDate]
      ,[ExpiryDate]
      ,[UnderwriterID]
      ,[MethodOfAcceptanceID]
      ,[RenewalStatusID]
      ,[RenewalStatusCode]
      ,[DateCreated]
      ,[DateExpired]
      ,[DateUpdated]
      ,[CurrentYN]
      ,[SourceSystemID])values( S.MasterNumber,S.MasterSeq,S.StatusID,S.ProductID,S.YearOfAccount,S.MasterReference,S.AssuredID,S.BrokerID,S.DepartmentID,S.BranchID,
		S.AreaID,S.Domicile,S.ClassID,S.CompanyID,S.InceptionDate,S.ExpiryDate,S.UnderwriterID,S.MethodOfAcceptanceID,
		S.RenewalStatusID,S.RenewalStatusCode,S.DateCreated,S.DateExpired,S.DateUpdated,S.CurrentYN,S.SourceSystemID)

--Update 
when matched and s.YearOfAccount<>d.YearOfAccount or s.MasterReference<>d.MasterReference or s.Domicile<>d.Domicile or s.InceptionDate<>d.InceptionDate or s.ExpiryDate<>d.ExpiryDate
then
update
set d.YearOfAccount=s.YearOfAccount , d.MasterReference=s.MasterReference ,d.Domicile=s.Domicile,d.InceptionDate=s.InceptionDate,d.ExpiryDate=s.ExpiryDate;
end

exec ETL_Load_DimPolicy_Merge_Incr_Loading

select * from TARDISDW_Merge.Dimension.Policy

 --_____________________________________________________________________________________________________________________________________________________________________
 --_____________________________________________________________________________________________________________________________________________________________________
-- 2. DimPolicySection

create proc ETL_Load_DimPolicySection_Merge_Incr_Loading
as 
begin

;WITH PolicyKeyMap AS (
    SELECT 
        PolicyID,
        MasterNumber +MasterSeq  AS PolicyKey,
		SourceSystemID
    FROM TARDISDW_Merge.Dimension.Policy
)


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
		GETDATE() AS DateCreated,STXR.DateExpired,GETDATE() AS DateUpdated,STXR.CurrentYN,d.SourceSystemID
		into #temp_PolicySection

		from Tardis_Stage.dbo.Stage_PolicySection S join PolicyKeyMap D 
		on S.PolicyKey=d.PolicyKey join TARDISDW_SP.dbo.SectionTypeXRef as STXR
		on S.SectionTypeCode=STXR.SectionTypeCode
		LEFT JOIN TARDISDW_Merge.Dimension.PolicySection Dim
		on s.SectionKey=Dim.SectionKey
		where dim.SectionKey is null





merge TARDISDW_Merge.Dimension.PolicySection as D
using #temp_PolicySection as S
on S.SectionKey=D.SectionKey

--INSERT
when not matched by target
then 
insert ([SectionKey]
      ,[SectionLongName]
      ,[SignedLinePercentage]
      ,[SignedOrderPercentage]
      ,[WrittenOrderPercentage]
      ,[SectionSequence]
      ,[SectionTypeID]
      ,[SectionSubTypeID]
      ,[SectionTitle]
      ,[PolicyID]
      ,[DateCreated]
      ,[DateExpired]
      ,[DateUpdated]
      ,[CurrentYN]
      ,[SourceSystemID])
	  values(S.[SectionKey]
      ,S.[SectionLongName]
      ,S.[SignedLinePercentage]
      ,S.[SignedOrderPercentage]
      ,S.[WrittenOrderPercentage]
      ,S.[SectionSequence]
      ,S.[SectionTypeID]
      ,S.[SectionSubTypeID]
      ,S.[SectionTitle]
      ,S.[PolicyID]
      ,S.[DateCreated]
      ,S.[DateExpired]
      ,S.[DateUpdated]
      ,S.[CurrentYN]
      ,S.[SourceSystemID])

--Update
when matched and S.SectionLongName<>D.SectionLongName OR S.SignedLinePercentage <>D.SignedLinePercentage OR S.SignedOrderPercentage<>D.SignedOrderPercentage OR 
S.WrittenOrderPercentage<>D.WrittenOrderPercentage OR S.SectionTitle<>D.SectionTitle OR S.SectionSequence<>D.SectionSequence
then 
update
set D.SectionLongName=S.SectionLongName,D.SignedLinePercentage =S.SignedLinePercentage,D.SignedOrderPercentage=S.SignedOrderPercentage,
D.WrittenOrderPercentage=S.WrittenOrderPercentage,D.SectionTitle=S.SectionTitle,D.SectionSequence=S.SectionSequence;

end

exec ETL_Load_DimPolicySection_Merge_Incr_Loading


select * from TARDISDW_SCD.Dimension.PolicySection
select * from TARDISDW_Merge.Dimension.PolicySection



--DateCreated = GETDATE() - when row is first inserted (warehouse load date).

--DateExpired = '9998-12-31' - acts as “still active” until a change arrives.

--DateUpdated = GETDATE() - when the row is last modified (ETL refresh timestamp).
  --_____________________________________________________________________________________________________________________________________________________________________
 --_____________________________________________________________________________________________________________________________________________________________________

--3.PolicyCoverage 

select * from TARDISDW_SCD.Dimension.PolicyCoverage
select * from TARDISDW_Merge.Dimension.PolicyCoverage --


select * from Tardis_Stage.dbo.Stage_PolicyCoverage

select * from TARDISDW_Merge.Dimension.PolicyCoverage --

create proc ETL_Load_PolicyCoverage_Merge_Incr_Loading
as
begin

if exists(select * from sys.tables where name='#Temp_PolicyCoverage')
drop table #Temp_PolicyCoverage

select  S.CoverageKey,
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
		into #Temp_PolicyCoverage
from Tardis_Stage.dbo.Stage_PolicyCoverage S  join TARDISDW_Merge.Dimension.PolicySection D on S.SectionKey=D.SectionKey
Left join TARDISDW_Merge.Dimension.PolicyCoverage as dest
on S.CoverageKey=dest.CoverageKey
--where Dest.CoverageKey is null


merge TARDISDW_Merge.Dimension.PolicyCoverage D
using #Temp_PolicyCoverage as S
on S.CoverageKey=D.CoverageKey

--For Insert 
when not matched by target
then 
insert ([CoverageKey]
      ,[PolicySectionID]
      ,[CoverageTypeID]
      ,[MinDeductible]
      ,[MaxDeductible]
      ,[CoverageSequence]
      ,[CoverageTitle]
      ,[DateCreated]
      ,[DateExpired]
      ,[DateUpdated]
      ,[CurrentYN]
      ,[SourceSystemID])
	  values (S.[CoverageKey]
      ,S.[PolicySectionID]
      ,S.[CoverageTypeID]
      ,S.[MinDeductible]
      ,S.[MaxDeductible]
      ,S.[CoverageSequence]
      ,S.[CoverageTitle]
      ,S.[DateCreated]
      ,S.[DateExpired]
      ,S.[DateUpdated]
      ,S.[CurrentYN]
      ,S.[SourceSystemID])

--For Update
when matched and D.MinDeductible<>S.MinDeductible or D.MaxDeductible<>S.MaxDeductible or D.CoverageSequence<>S.CoverageSequence or D.CoverageTitle<>S.CoverageTitle
then 
update
set D.MinDeductible=S.MinDeductible,D.MaxDeductible=S.MaxDeductible,D.CoverageSequence=S.CoverageSequence,D.CoverageTitle=S.CoverageTitle;
end


select * from Tardis_Stage.dbo.Stage_PolicySection

exec  ETL_Load_PolicyCoverage_Merge_Incr_Loading

select * from TARDISDW_Merge.Dimension.PolicyCoverage --
select * from TARDISDW_SCD.Dimension.PolicyCoverage

 --_____________________________________________________________________________________________________________________________________________________________________
 --_____________________________________________________________________________________________________________________________________________________________________
 --4. DimTransactionType

 create proc ETL_Load_TransactionType_Merge_Incr_Loading
as 
begin

if exists (select * from sys.tables where name='#temp_TransactionType')
drop table #temp_TransactionType

 select S.TransactionTypeCode,
		S.TransactionTypeDescription,
		S.DateCreated,
		S.DateExpired,
		S.DateUpdated,
		S.CurrentYN,
		S.SourceSystemID 
		into #temp_TransactionType
 from Tardis_Stage.dbo.Stage_TransactionType as S left join TARDISDW_Merge.Dimension.TransactionType as D
 on S.TransactionTypeCode=D.TransactionTypeCode
 where d.TransactionTypeCode is null

 --Merge Statement
merge TARDISDW_Merge.Dimension.TransactionType as D
using #temp_TransactionType as S
on s.TransactionTypeCode=D.TransactionTypeCode

--Insert
when not matched by target
then 
insert ([TransactionTypeCode]
      ,[TransactionTypeDescription]
      ,[DateCreated]
      ,[DateExpired]
      ,[DateUpdated]
      ,[CurrentYN]
      ,[SourceSystemID])
	  values
	  (s.[TransactionTypeCode]
      ,s.[TransactionTypeDescription]
      ,s.[DateCreated]
      ,s.[DateExpired]
      ,s.[DateUpdated]
      ,s.[CurrentYN]
      ,s.[SourceSystemID])

--Update
when matched and S.TransactionTypeDescription<>D.TransactionTypeDescription
then 
update
set D.TransactionTypeDescription=S.TransactionTypeDescription;

END

EXEC ETL_Load_TransactionType_Merge_Incr_Loading

select * from Dimension.TransactionType
--_____________________________________________________________________________________________________________________________________________________________________
 --_____________________________________________________________________________________________________________________________________________________________________
--5.DimRevenue
create proc ETL_Load_RevenueType_Merge_Incr_Loading
as 
begin

if exists (select * from sys.tables where name='#temp_Revenue')
drop table #temp_Revenue

select s.RevenueTypeCode,
		s.RevenueTypeName,
		s.RevenueTypeDescription,
		s.DateCreated,
		s.DateExpired,
		s.DateUpdated,
		s.CurrentYN,
		s.SourceSystemID
		into #temp_Revenue
		from Tardis_Stage.dbo.Stage_RevenueType as S
 left join TARDISDW_Merge.Dimension.RevenueType as D 
 on D.RevenueTypeCode=s.RevenueTypeCode
 --where D.RevenueTypeCode is null

--Merge Statement
merge TARDISDW_Merge.Dimension.RevenueType D
using #temp_Revenue as S
on S.RevenueTypeCode=D.RevenueTypeCode


--Insert 
when not matched by target
then 
insert ([RevenueTypeCode]
      ,[RevenueTypeName]
      ,[RevenueTypeDescription]
      ,[DateCreated]
      ,[DateExpired]
      ,[DateUpdated]
      ,[CurrentYN]
      ,[SourceSystemID])
	  values(S.RevenueTypeCode
      ,S.RevenueTypeName
      ,S.RevenueTypeDescription
      ,S.DateCreated
      ,S.DateExpired
      ,S.DateUpdated
      ,S.CurrentYN
      ,S.SourceSystemID)


--Update
When matched and D.RevenueTypeName<>S.RevenueTypeName or D.RevenueTypeDescription<>S.RevenueTypeDescription
then 
update
set d.RevenueTypeName=s.RevenueTypeName,d.RevenueTypeDescription=s.RevenueTypeDescription;

end


  exec  ETL_Load_RevenueType_Merge_Incr_Loading

 select * from [TARDISDW_Merge].Dimension.RevenueType

 --_____________________________________________________________________________________________________________________________________________________________________
 --_____________________________________________________________________________________________________________________________________________________________________
--6. Dim_Currency

create proc ETL_Load_DimCurrency_Merge_Incr_Loading
as 
begin

SELECT 
    S.CurrCode, 
    S.CurrDescription, 
    S.CreateDate, 
    S.UpdateDate, 
    S.BatchID, 
    S.SourceSystemID, 
    S.CurrentYN
	into #Temp_Currency
FROM  Tardis_Stage.dbo.Stage_Currency AS S
LEFT JOIN TARDISDW_Merge.Dimension.Currency AS D
    ON D.CurrCode = S.CurrCode  -- Business Key

 
-- Merge Statement

if exists (select * from sys.tables where name = '#Temp_Currency' )
drop table #Temp_Currency

merge TARDISDW_Merge.Dimension.Currency AS D
using #Temp_Currency as S
on S.Currcode=D.Currcode

-- For Insert
when not matched by target
then 
insert ([CurrCode]
      ,[CurrDescription]
      ,[CreateDate]
      ,[UpdateDate]
      ,[BatchID]
      ,[SourceSystemID]
      ,[CurrentYN])
	  values (S.[CurrCode]
      ,S.[CurrDescription]
      ,S.[CreateDate]
      ,S.[UpdateDate]
      ,S.[BatchID]
      ,S.[SourceSystemID]
      ,S.[CurrentYN])

--For Update
when matched and S.CurrDescription<>D.CurrDescription
then 
update
set D.CurrDescription=S.CurrDescription ;
end


exec ETL_Load_DimCurrency_Merge_Incr_Loading


select * from Dimension.Currency--3
 --_____________________________________________________________________________________________________________________________________________________________________
 --_____________________________________________________________________________________________________________________________________________________________________
 --_____________________________________________________________________________________________________________________________________________________________________
 --_____________________________________________________________________________________________________________________________________________________________________
--_____________________________________________________________________________________________________________________________________________________________________
 --_____________________________________________________________________________________________________________________________________________________________________
--_____________________________________________________________________________________________________________________________________________________________________
 --_____________________________________________________________________________________________________________________________________________________________________
--_____________________________________________________________________________________________________________________________________________________________________
 --_____________________________________________________________________________________________________________________________________________________________________
--_____________________________________________________________________________________________________________________________________________________________________
 --_____________________________________________________________________________________________________________________________________________________________________


---------------------------------------------------Fact Tables --------------------------------------------------------------

select * from Tardis_Stage.dbo.Stage_Premium
select * from TARDISDW_Merge.Fact.Premium

select  * from TARDISDW_Merge.Dimension.Date



--1.Fact Premium

create proc ETL_Load_FactPremium_Merge_Incr_Loading
as
begin
select 	S.SourceIdentifier,T.TransactionTypeID,R.RevenueTypeID,CC.CurrencyID as OriginalCurrencyID,C.CurrencyID as SettlementCurrenyID,P.PolicyCoverageID,
 DA.TimeID,S.BasePremiumAmount,S.SignedPremiumAmount,S.PredictedPremiumAmount,S.WrittenPremiumAmount,T.SourceSystemID,getdate() as DateCreated,getdate() as DateUpdated,P.CurrentYN
 into #Temp_FactPremium
 from Tardis_Stage.dbo.Stage_Premium S join TARDISDW_Merge.Dimension.PolicyCoverage P 
 on S.CoverageKey=p.CoverageKey join TARDISDW_Merge.Dimension.TransactionType T 
 on T.TransactionTypeCode=S.TransactionCode join TARDISDW_Merge.Dimension.RevenueType R
 on R.RevenueTypeCode=S.RevenueType join TARDISDW_Merge.Dimension.Currency C
 on C.CurrCode=s.CurrCode join TARDISDW_Merge.Dimension.Currency CC 
 on CC.CurrCode=S.CurrCode join TARDISDW_Merge.Dimension.Date Da
 on Da.Date=s.Date Left join TARDISDW_Merge.Fact.Premium as Dest
 on Dest.SourceIdentifier=S.SourceIdentifier
 where Dest.SourceIdentifier is null;

 merge TARDISDW_Merge.Fact.Premium as D
 using #Temp_FactPremium as S
 on S.SourceIdentifier=D.SourceIdentifier

 --For Insert
 when not matched by target 
 then 
 insert ([SourceIdentifier]
      ,[TransactionTypeID]
      ,[RevenueTypeID]
      ,[OriginalCurrencyID]
      ,[SettlementCurrenyID]
      ,[PolicyCoverageID]
      ,[TimeID]
      ,[BasePremiumAmount]
      ,[SignedPremiumAmount]
      ,[PredictedPremiumAmount]
      ,[WrittenPremiumAmount]
      ,[SourceSystemID]
      ,[DateCreated]
      ,[DateUpdated]
      ,[CurrentYN])
	  values(S.[SourceIdentifier]
      ,S.[TransactionTypeID]
      ,S.[RevenueTypeID]
      ,S.[OriginalCurrencyID]
      ,S.[SettlementCurrenyID]
      ,S.[PolicyCoverageID]
      ,S.[TimeID]
      ,S.[BasePremiumAmount]
      ,S.[SignedPremiumAmount]
      ,S.[PredictedPremiumAmount]
      ,S.[WrittenPremiumAmount]
      ,S.[SourceSystemID]
      ,S.[DateCreated]
      ,S.[DateUpdated]
      ,S.[CurrentYN])

--For Update
when matched and D.BasePremiumAmount<>S.BasePremiumAmount or D.SignedPremiumAmount<>S.SignedPremiumAmount or 
				 D.PredictedPremiumAmount<>S.PredictedPremiumAmount or D.WrittenPremiumAmount<>S.WrittenPremiumAmount
then
update
set D.BasePremiumAmount=S.BasePremiumAmount,D.SignedPremiumAmount=S.SignedPremiumAmount, 
	D.PredictedPremiumAmount=S.PredictedPremiumAmount,D.WrittenPremiumAmount=S.WrittenPremiumAmount;

end

exec ETL_Load_FactPremium_Merge_Incr_Loading

select * from TARDISDW_Merge.Fact.Premium

--_____________________________________________________________________________________________________________________________________________________________________
 --_____________________________________________________________________________________________________________________________________________________________________
--2. Fact Limit

create proc ETL_Load_FactLimit_Merge_Incr_Loading
as
begin
IF exists(select * from sys.tables where name ='#TempLimit')
drop table #TempLimit
select 	S.SourceIdentifier,T.TransactionTypeID,C.CurrencyID as OriginalCurrencyID,CC.CurrencyID as SettlementCurrencyID,P.PolicyCoverageID,D.TimeID,S.FXRate,
		S.LimitAmount,C.SourceSystemID,T.DateCreated,T.DateUpdated,T.CurrentYN
		into #TempLimit

 from Tardis_Stage.dbo.Stage_Limit S join TARDISDW_Merge.Dimension.TransactionType T
 on T.TransactionTypeCode=S.TransactionCode join TARDISDW_Merge.Dimension.Currency C
 on C.CurrCode=S.CurrCode join TARDISDW_Merge.Dimension.Currency CC
 on CC.CurrCode=S.CurrCode join TARDISDW_Merge.Dimension.PolicyCoverage P
 on P.CoverageKey=S.CoverageKey join TARDISDW_Merge.Dimension.Date D
 on D.Date=S.Date Left JOIN TARDISDW_Merge.Fact.Limit L
 on L.SourceIdentifier=S.SourceIdentifier
 where L.SourceIdentifier is null
  
merge TARDISDW_Merge.Fact.Limit as D
using #TempLimit as S
on S.SourceIdentifier=D.SourceIdentifier

--For Inert
when not matched by target
then
insert ([SourceIdentifier]
      ,[TransactionTypeID]
      ,[OriginalCurrencyID]
      ,[SettlementCurrencyID]
      ,[PolicyCoverageID]
      ,[TimeID]
      ,[FXRate]
      ,[LimitAmount]
      ,[SourceSystemID]
      ,[DateCreated]
      ,[DateUpdated]
      ,[CurrentYN])
	  values (S.[SourceIdentifier]
      ,S.[TransactionTypeID]
      ,S.[OriginalCurrencyID]
      ,S.[SettlementCurrencyID]
      ,S.[PolicyCoverageID]
      ,S.[TimeID]
      ,S.[FXRate]
      ,S.[LimitAmount]
      ,S.[SourceSystemID]
      ,S.[DateCreated]
      ,S.[DateUpdated]
      ,S.[CurrentYN])
 
 --For Update
 when matched and D.FXRate<>S.FXRate or D.LimitAmount<>S.FXRate
 then
 update
 set D.FXRate=S.FXRate,D.LimitAmount=S.FXRate;
 end 

 exec ETL_Load_FactLimit_Merge_Incr_Loading

 SELECT * FROM TARDISDW_Merge.Fact.Limit

 --_____________________________________________________________________________________________________________________________________________________________________
 --_____________________________________________________________________________________________________________________________________________________________________
--2. Fact Deduction


create proc ETL_Load_FactDeduction_Merge_Incr_Loading
as
begin

IF exists(select * from sys.tables where name='#TempDeduction')
drop table #TempDeduction

select S.SourceIdentifier,T.TransactionTypeID,R.RevenueTypeID,P.PolicyCoverageID,C.CurrencyID,D.TimeID,S.DeductionAmount,S.FXRate,
s.Percentgage as  DeductionPercentage,R.SourceSystemID,T.DateCreated,T.DateUpdated,T.CurrentYN
into #TempDeduction
from Tardis_Stage.dbo.Stage_Deduction  S join TARDISDW_Merge.Dimension.TransactionType as T
on T.TransactionTypeCode=S.TransactionCode join TARDISDW_Merge.Dimension.RevenueType as R
on R.RevenueTypeCode=S.RevenueTypeCode join TARDISDW_Merge.Dimension.PolicyCoverage as P
on P.CoverageKey =S.CoverageKey join TARDISDW_Merge.Dimension.Currency as C
on C.CurrCode=s.CurrCode join TARDISDW_Merge.Dimension.Date D
on D.Date=S.Date left join TARDISDW_Merge.Fact.Deduction as Deduc
on S.SourceIdentifier=Deduc.SourceIdentifier
where Deduc.SourceIdentifier is null

--Merge Statement
merge TARDISDW_Merge.Fact.Deduction as D
using #TempDeduction as S
on S.SourceIdentifier=D.SourceIdentifier

--For Insert
when not matched by target
then 
insert([SourceIdentifier]
      ,[TransactionTypeID]
      ,[RevenueTypeID]
      ,[PolicyCoverageID]
      ,[CurrencyID]
      ,[TimeID]
      ,[DeductionAmount]
      ,[FXRate]
      ,[DeductionPercentage]
      ,[SourceSystemID]
      ,[DateCreated]
      ,[DateUpdated]
      ,[CurrentYN])
	  values(S.[SourceIdentifier]
      ,S.[TransactionTypeID]
      ,S.[RevenueTypeID]
      ,S.[PolicyCoverageID]
      ,S.[CurrencyID]
      ,S.[TimeID]
      ,S.[DeductionAmount]
      ,S.[FXRate]
      ,S.[DeductionPercentage]
      ,S.[SourceSystemID]
      ,S.[DateCreated]
      ,S.[DateUpdated]
      ,S.[CurrentYN])

--For Update
when matched and D.DeductionAmount<>S.DeductionAmount or D.FXRate<>S.FXRate or D.DeductionPercentage<>S.DeductionPercentage
then
update
set D.DeductionAmount=S.DeductionAmount,D.FXRate=S.FXRate,D.DeductionPercentage=S.DeductionPercentage ;
end


 exec  ETL_Load_FactDeduction_Merge_Incr_Loading


  SELECT * FROM TARDISDW_Merge.Fact.Deduction

--````````````````````````````````````````````````````The END```````````````````````````````````````````````````````````````


  --Deduction percentage


  select * from TARDISDW_Merge.Dimension.TransactionType
  select * from TARDISDW_Merge.Dimension.RevenueType
  select * from TARDISDW_Merge.Dimension.PolicyCoverage
  select * from TARDISDW_Merge.Dimension.Currency
  select * from TARDISDW_Merge.Dimension.Date
  select * from TARDISDW_Merge.Fact.Deduction




















	  SELECT * FROM TARDISDW_Merge.Fact.Premium --Need date

	  	  SELECT * FROM Tardis_Stage.dbo.Stage_Premium  --No date in stage

/*

		  select * from TARDISDW_Merge.Dimension.PolicyCoverage
PolicyCoverageID	CoverageKey	PolicySectionID	CoverageTypeID	MinDeductible	MaxDeductible	CoverageSequence	CoverageTitle	DateCreated	DateExpired	DateUpdated	CurrentYN	SourceSystemID
1	Q05WMX001001007	1	1	100.00	100.00	1	Test	2025-09-18 13:54:54.937	9998-12-31 00:00:00.000	2025-09-18 13:54:54.937	1	5
2	Q05WMY001001008	2	1	100.00	100.00	1	Test	2025-09-18 13:54:54.937	9998-12-31 00:00:00.000	2025-09-18 13:54:54.937	1	5

		  select * from TARDISDW_Merge.Dimension.TransactionType
TransactionTypeID	TransactionTypeCode	TransactionTypeDescription	DateCreated	DateExpired	DateUpdated	CurrentYN	SourceSystemID
1	111	transaction	2015-03-30 18:12:09.540	2015-03-29 18:12:09.540	2015-03-30 18:12:09.540	1	1


		  select * from TARDISDW_Merge.Dimension.RevenueType
RevenueTypeID	RevenueTypeCode	RevenueTypeName	RevenueTypeDescription	DateCreated	DateExpired	DateUpdated	CurrentYN	SourceSystemID
1	PRM	kk	project mngr	2015-03-30 14:35:21.843	9998-12-31 00:00:00.000	2015-03-30 14:35:21.843	1	-1
2	BRK	ff	NULL	2015-04-03 18:59:16.880	9998-12-31 00:00:00.000	2015-04-03 18:59:16.880	1	-1

		  select * from TARDISDW_Merge.Dimension.Currency
CurrencyID	CurrCode	CurrDescription	CreateDate	UpdateDate	BatchID	SourceSystemID	CurrentYN
1	EUR	Europecurrency	2015-03-30 13:59:26.193	2015-03-30 13:59:26.193	-1	-1	1
2	INR	IndainRupee	2015-03-30 13:59:31.013	2015-03-30 13:59:31.013	-1	-1	1
3	USD	USdollor	2015-03-30 13:59:37.053	2015-03-30 13:59:37.053	-1	-1	1


*/



-- All the views for SSAS are here 
--1. Brach Wise Revenue (Premium amount)

select * from BranchXRef B  join Dimension.Policy P on B.BranchID=P.BranchID
join Dimension.PolicySection D on D.PolicyID=P.PolicyID 
join Dimension.PolicyCoverage C on C.PolicySectionID = D.PolicySectionID
join Fact.Deduction F on F.PolicyCoverageID=C.PolicyCoverageID
join Dimension.RevenueType R on R.RevenueTypeID=F.RevenueTypeID
join Fact.Premium FP on FP.RevenueTypeID=R.RevenueTypeID














