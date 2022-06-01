--CREATE TABLE IF NOT EXISTS "lla_cco_int_stg"."cwp_fix_stg_dashboardinput_dinamico_RJ_v3" AS
WITH
Convergente AS(
SELECT DISTINCT *,DATE_TRUNC('MONTH', DATE_PARSE(CAST(Date AS VARCHAR(10)), '%Y%m%d')) as Mes
FROM "lla_cco_int_ext"."cwp_con_ext_base"
WHERE telefonia='Pospago' AND "unidad de negocio"='1. B2C'
 AND DATE_TRUNC('MONTH', DATE_PARSE(CAST(Date AS VARCHAR(10)), '%Y%m%d'))=DATE('2022-02-01') or DATE_TRUNC('MONTH', DATE_PARSE(CAST(Date AS VARCHAR(10)), '%Y%m%d'))=DATE('2022-01-01')
)

,FixedUsefulFields AS(
SELECT DISTINCT LOAD_DT
,ACT_ACCT_CD AS FixedAccount,ACT_CONTACT_PHONE_3 AS CONTACTO
,FI_OUTST_AGE,MAX(CAST(CAST(act_cust_strt_dt AS TIMESTAMP) AS DATE)) AS MaxStart, round(FI_TOT_MRC_AMT,0) AS Fixed_MRC
,Case When pd_bb_accs_media = 'FTTH' Then 'FTTH'
        When pd_bb_accs_media = 'HFC' Then 'HFC'
        when pd_TV_accs_media = 'FTTH' AND pd_bb_accs_media  IS NULL Then 'FTTH'
        when pd_TV_accs_media = 'HFC' AND pd_bb_accs_media  IS NULL Then 'HFC'
        when pd_VO_accs_media = 'FTTH' AND pd_bb_accs_media  IS NULL AND pd_TV_accs_media IS NULL Then 'FTTH'
        when pd_VO_accs_media = 'HFC' AND pd_bb_accs_media  IS NULL AND pd_TV_accs_media IS NULL Then 'HFC'
    ELSE 'COPPER' end as TechFlag
,CASE WHEN pd_bb_prod_cd IS NOT NULL AND CAST(pd_bb_prod_cd AS VARCHAR(50)) <> '' THEN 1 ELSE 0 END AS numBB
,CASE WHEN pd_tv_prod_cd IS NOT NULL AND CAST(pd_tv_prod_cd  AS VARCHAR(50)) <> '' THEN 1 ELSE 0 END AS numTV
,CASE WHEN pd_vo_prod_cd IS NOT NULL AND CAST(pd_vo_prod_cd AS VARCHAR(50)) <> '' THEN 1 ELSE 0 END AS numVO
,CASE WHEN pd_bb_prod_cd IS NOT NULL AND CAST(pd_bb_prod_cd AS VARCHAR(50)) <> '' THEN act_acct_cd ELSE NULL END AS BB
,CASE WHEN pd_tv_prod_cd IS NOT NULL AND CAST(pd_tv_prod_cd  AS VARCHAR(50)) <> '' THEN act_acct_cd ELSE NULL END AS TV
,CASE WHEN pd_vo_prod_cd IS NOT NULL AND CAST(pd_vo_prod_cd AS VARCHAR(50)) <> '' THEN act_acct_cd ELSE NULL END AS VO,
CASE WHEN evt_frst_sale_chnl = 'CALL CENTER' THEN 'Tele Sales'
        WHEN evt_frst_sale_chnl = 'Negocios Regionales' or evt_frst_sale_chnl ='AM REGIONAL'THEN 'Regionales'
        WHEN evt_frst_sale_chnl = 'Dealers' THEN 'Agencias'
        WHEN evt_frst_sale_chnl = 'TIENDAS' or evt_frst_sale_chnl = 'Tiendas'  THEN 'Stores'
        WHEN evt_frst_sale_chnl = 'D2D' or evt_frst_sale_chnl = 'Door 2 Door B2C'  THEN 'D2D'
        WHEN evt_frst_sale_chnl in ( 'Alianzas', 'Promotores', 'Ventas Corporativas') THEN 'Other'
        WHEN evt_frst_sale_chnl = 'Ventas Web' then 'WEB'
         WHEN evt_frst_sale_chnl is null THEN 'No Channel'
         Else NULL 
         END AS FIRST_SALES_CHNL,
CASE WHEN evt_lst_sale_chnl = 'CALL CENTER' THEN 'Tele Sales'
        WHEN evt_lst_sale_chnl = 'Negocios Regionales'  or evt_lst_sale_chnl ='AM REGIONAL'THEN 'Regionales'
        WHEN evt_lst_sale_chnl = 'Dealers' THEN 'Agencias'
        WHEN evt_lst_sale_chnl = 'TIENDAS' or evt_lst_sale_chnl = 'Tiendas'  THEN 'Stores'
        WHEN evt_lst_sale_chnl = 'D2D' or evt_lst_sale_chnl = 'Door 2 Door B2C'  THEN 'D2D'
        WHEN evt_lst_sale_chnl in ( 'Alianzas', 'Promotores', 'Ventas Corporativas') THEN 'Other'
        WHEN evt_lst_sale_chnl = 'Ventas Web' then 'WEB'
         WHEN evt_lst_sale_chnl is null THEN 'No Channel'
         Else NULL 
         END AS LAST_SALES_CHNL,
PD_BB_PROD_CD, pd_tv_prod_cd, PD_VO_PROD_CD, pd_mix_nm,pd_mix_cd
FROM "lla_cco_int_stg"."cwp_fix_union_dna"
WHERE PD_MIX_CD<>'0P'AND act_cust_typ_nm = 'Residencial' 
AND date(load_dt) between (DATE('2022-02-01') + interval '1' MONTH - interval '1' DAY - interval '3' MONTH) AND  (DATE('2022-02-01') + interval '1' MONTH - interval '1' DAY + interval '3' MONTH)
GROUP BY LOAD_DT,2,3,FI_OUTST_AGE,6, 7,8, 9, 10, 11,12,13,14,15,PD_BB_PROD_CD,pd_tv_prod_cd,PD_VO_PROD_CD,pd_mix_nm,pd_mix_cd
)
,HardBundleFlag AS(
SELECT DISTINCT LOAD_DT, ACT_ACCT_CD
FROM "lla_cco_int_stg"."cwp_fix_union_dna"
WHERE act_cust_typ_nm = 'Residencial'
AND ((PD_VO_PROD_CD = 1719 AND PD_BB_PROD_CD = 1743) OR
(PD_VO_PROD_CD = 1719 AND PD_BB_PROD_CD = 1744) OR
(PD_VO_PROD_CD = 1718 AND PD_BB_PROD_CD = 1645))
)

,FixedActive_BOM AS(
SELECT f.LOAD_DT as Fix_B_Date, DATE_TRUNC('MONTH', DATE_ADD('MONTH', 1, DATE(f.LOAD_dt))) AS FixedMonth,
FixedAccount as FixedAccount_BOM, Contacto AS Fixed_B_Phone, FI_OUTST_AGE as B_Overdue, MaxStart as Fixed_B_MaxStart,
First_sales_chnl as  First_sales_chnl_bom, Last_sales_chnl as Last_sales_chnl_bom
,CASE WHEN DATE_DIFF('DAY', MaxStart, DATE(f.LOAD_DT))<=180 THEN 'Early-Tenure'
      WHEN DATE_DIFF('DAY', MaxStart, DATE(f.LOAD_DT))>180 THEN 'Late-Tenure' END AS B_FixedTenure
,round(Fixed_MRC,0) as B_Fixed_MRC, TechFlag as B_TechFlag, (numBB+numTV+numVO) as B_NumRGUs
,CASE WHEN NumBB = 1 AND NumTV = 0 AND NumVO = 0 THEN 'BO'
    WHEN NumBB = 0 AND NumTV = 1 AND NumVO = 0 THEN 'TV'
    WHEN NumBB = 0 AND NumTV = 0 AND NumVO = 1 THEN 'VO'
    WHEN NumBB = 1 AND NumTV = 1 AND NumVO = 0 THEN 'BO+TV'
    WHEN NumBB = 1 AND NumTV = 0 AND NumVO = 1 THEN 'BO+VO'
    WHEN NumBB = 0 AND NumTV = 1 AND NumVO = 1 THEN 'VO+TV'
    WHEN NumBB = 1 AND NumTV = 1 AND NumVO = 1 THEN 'BO+VO+TV'
    END AS B_MixName_Adj,
    CASE WHEN (NumBB = 1 AND NumTV = 0 AND NumVO = 0) OR  (NumBB = 0 AND NumTV = 1 AND NumVO = 0) OR (NumBB = 0 AND NumTV = 0 AND NumVO = 1)  THEN '1P'
    WHEN (NumBB = 1 AND NumTV = 1 AND NumVO = 0) OR (NumBB = 1 AND NumTV = 0 AND NumVO = 1) OR (NumBB = 0 AND NumTV = 1 AND NumVO = 1) THEN '2P'
    WHEN (NumBB = 1 AND NumTV = 1 AND NumVO = 1) THEN '3P' END AS B_MixCode_Adj, BB AS B_BB, TV AS B_TV, VO AS B_VO
    ,PD_BB_PROD_CD AS B_bbCode, PD_TV_PROD_CD AS B_tvCode, PD_VO_PROD_CD AS B_voCode,
    CASE WHEN h.ACT_ACCT_CD IS NOT NULL THEN 'Hard FMC'
    ELSE 'TBD' END AS B_Hard_FMC_Flag
    FROM FixedUsefulFields f LEFT JOIN HardBundleFlag h ON f.fixedaccount = h.ACT_ACCT_CD AND f.LOAD_DT = h.LOAD_DT
    WHERE f.LOAD_dt = --'2022-02-02' --DATE(f.dt) = date(PRUEADINAMICA_DT)
     date_trunc('MONTH', DATE(f.LOAD_dt)) + interval '1' MONTH - interval '1' day
    AND (CAST(FI_OUTST_AGE AS INTEGER)<90 OR FI_OUTST_AGE IS NULL)
)

,FixedActive_EOM AS(
SELECT f.LOAD_DT as Fix_E_Date, DATE_TRUNC('MONTH', DATE(f.LOAD_dt)) AS FixedMonth,
FixedAccount as FixedAccount_EOM, Contacto AS Fixed_E_Phone, FI_OUTST_AGE as E_Overdue, MaxStart as Fixed_E_MaxStart,
First_sales_chnl as  First_sales_chnl_eom, Last_sales_chnl as Last_sales_chnl_eom
,CASE WHEN DATE_DIFF('DAY', DATE(MaxStart), DATE(f.LOAD_DT))<=180 THEN 'Early-Tenure'
      WHEN DATE_DIFF('DAY', DATE(MaxStart), DATE(f.LOAD_DT))>180 THEN 'Late-Tenure' END AS E_FixedTenure
,round(Fixed_MRC,0) as E_Fixed_MRC, TechFlag as E_TechFlag, (numBB+numTV+numVO) as E_NumRGUs
,CASE WHEN NumBB = 1 AND NumTV = 0 AND NumVO = 0 THEN 'BO'
    WHEN NumBB = 0 AND NumTV = 1 AND NumVO = 0 THEN 'TV'
    WHEN NumBB = 0 AND NumTV = 0 AND NumVO = 1 THEN 'VO'
    WHEN NumBB = 1 AND NumTV = 1 AND NumVO = 0 THEN 'BO+TV'
    WHEN NumBB = 1 AND NumTV = 0 AND NumVO = 1 THEN 'BO+VO'
    WHEN NumBB = 0 AND NumTV = 1 AND NumVO = 1 THEN 'VO+TV'
    WHEN NumBB = 1 AND NumTV = 1 AND NumVO = 1 THEN 'BO+VO+TV'
    END AS E_MixName_Adj,
    CASE WHEN (NumBB = 1 AND NumTV = 0 AND NumVO = 0) OR  (NumBB = 0 AND NumTV = 1 AND NumVO = 0) OR (NumBB = 0 AND NumTV = 0 AND NumVO = 1)  THEN '1P'
    WHEN (NumBB = 1 AND NumTV = 1 AND NumVO = 0) OR (NumBB = 1 AND NumTV = 0 AND NumVO = 1) OR (NumBB = 0 AND NumTV = 1 AND NumVO = 1) THEN '2P'
    WHEN (NumBB = 1 AND NumTV = 1 AND NumVO = 1) THEN '3P' END AS E_MixCode_Adj, BB AS E_BB, TV AS E_TV, VO AS E_VO
    ,PD_BB_PROD_CD AS E_bbCode, PD_TV_PROD_CD AS E_tvCode, PD_VO_PROD_CD AS E_voCode,
    CASE WHEN h.ACT_ACCT_CD IS NOT NULL THEN 'Hard FMC'
    ELSE 'TBD' END AS E_Hard_FMC_Flag
    FROM FixedUsefulFields f LEFT JOIN HardBundleFlag h ON f.fixedaccount = h.ACT_ACCT_CD AND f.LOAD_DT = h.LOAD_DT
    WHERE DATE(f.LOAD_dt) = date_trunc('MONTH', DATE(f.LOAD_dt)) + interval '1' MONTH - interval '1' day
    AND (CAST(FI_OUTST_AGE AS INTEGER)<90 OR FI_OUTST_AGE IS NULL)
)

,CustomerStatus AS(
  SELECT DISTINCT
  CASE WHEN (FixedAccount_BOM IS NOT NULL AND FixedAccount_EOM IS NOT NULL) OR (FixedAccount_BOM IS NOT NULL AND FixedAccount_EOM IS NULL) THEN b.FixedMonth
      WHEN (FixedAccount_BOM IS NULL AND FixedAccount_EOM IS NOT NULL) THEN e.FixedMonth
  END AS FixedMonth,
      CASE WHEN (FixedAccount_BOM IS NOT NULL AND FixedAccount_EOM IS NOT NULL) OR (FixedAccount_BOM IS NOT NULL AND FixedAccount_EOM IS NULL) THEN FixedAccount_BOM
      WHEN (FixedAccount_BOM IS NULL AND FixedAccount_EOM IS NOT NULL) THEN FixedAccount_EOM
  END AS FixedAccount
  ,CASE WHEN FixedAccount_BOM IS NOT NULL THEN 1 ELSE 0 END AS F_ActiveBOM
  ,CASE WHEN FixedAccount_EOM IS NOT NULL THEN 1 ELSE 0 END AS F_ActiveEOM,
  -- b.*, e.*
  Fix_B_Date, FixedAccount_BOM, Fixed_B_Phone, B_Overdue, Fixed_B_MaxStart, B_FixedTenure, B_Fixed_MRC, B_TechFlag, B_NumRGUs, B_MixName_Adj, B_MixCode_Adj, B_BB,B_TV,B_VO,B_bbCode, B_tvCode, B_voCode, B_Hard_FMC_Flag, Fix_E_Date, Fixed_E_Phone, E_Overdue, Fixed_E_MaxStart, E_FixedTenure, E_Fixed_MRC, E_TechFlag, E_NumRGUs, E_MixName_Adj, E_MixCode_Adj, E_BB,E_TV,E_VO,E_bbCode, E_tvCode, E_voCode, E_Hard_FMC_Flag,
  First_sales_chnl_bom, Last_sales_chnl_bom, First_sales_chnl_eom, Last_sales_chnl_eom
  FROM FixedActive_BOM b FULL OUTER JOIN FixedActive_EOM e
  ON b.FixedAccount_BOM = e.FixedAccount_EOM AND b.FixedMonth = e.FixedMonth
)
,MainMovementBase AS(
SELECT a.*,
CASE
WHEN (E_NumRGUs - B_NumRGUs) = 0 THEN '1.SameRGUs'
WHEN (E_NumRGUs - B_NumRGUs) > 0 THEN '2.Upsell'
WHEN (E_NumRGUs - B_NumRGUs) < 0 THEN '3.Downsell'
WHEN (B_NumRGUs IS NULL AND E_NumRGUs > 0 AND DATE_TRUNC ('MONTH', Fixed_E_MaxStart) =  FixedMonth) 
THEN '4.New Customer'
WHEN (B_NumRGUs IS NULL AND E_NumRGUs > 0 AND DATE_TRUNC ('MONTH', Fixed_E_MaxStart) <> FixedMonth) 
THEN '5.Come Back to Life'
WHEN (B_NumRGUs > 0 AND E_NumRGUs IS NULL) THEN '6.Null last day'
WHEN B_NumRGUs IS NULL AND E_NumRGUs IS NULL THEN '7.Always null'
END AS FixedMainMovement
FROM CustomerStatus a
)
,SpinMovementBase AS(
  SELECT b.*,
  CASE WHEN FixedMainMovement = '1.SameRGUs' AND (E_Fixed_MRC - B_Fixed_MRC) > 0 THEN '1. Up-spin'
  WHEN FixedMainMovement = '1.SameRGUs' AND (E_Fixed_MRC - B_Fixed_MRC) < 0 THEN '2. Down-spin'
  ELSE '3. No Spin' END AS FixedSpinMovement
  FROM MainMovementBase b
)

-- ################ Voluntary Churn ###############################################
,SO_flag AS(
Select distinct 
date_trunc('Month', date(completed_date)) as month,date(completed_date) as EndDate,date(order_start_date) as StartDate
,cease_reason_code, cease_reason_desc,cease_reason_group
,CASE 
 WHEN cease_reason_code IN ('1','3','4','5','6','7','8','10','12','13','14','15','16','18','20','23','25','26','29','30','31','34','35','36','37','38','39','40','41','42','43','45','46','47','50','51','52','53','54','56','57','70','71','73','75','76','77','78','79','80','81','82','83','84','85','86','87','88','89','90','91') THEN 'Voluntario'
 WHEN cease_reason_code IN('2','74') THEN 'Involuntario'
 WHEN (cease_reason_code = '9' AND cease_reason_desc='CAMBIO DE TECNOLOGIA') OR (cease_reason_code IN('32','44','55','72')) THEN 'Migracion'
 WHEN cease_reason_code = '9' AND cease_reason_desc<>'CAMBIO DE TECNOLOGIA' THEN 'Voluntario'
ELSE NULL END AS DxType
,account_id
,lob_vo_count,lob_bb_count,lob_tv_count
from "db-analytics-prod"."so_hdr_cwp" 
where order_type = 'DEACTIVATION' AND ACCOUNT_TYPE='R' AND ORDER_STATUS='COMPLETED'
)
,RGUsFlag_SO AS(
SELECT Month,StartDate,account_id,DxType
,CASE WHEN lob_vo_count>0 THEN 1 ELSE 0 END AS VO_Churn
,CASE WHEN lob_bb_count>0 THEN 1 ELSE 0 END AS BB_Churn
,CASE WHEN lob_tv_count>0 THEN 1 ELSE 0 END AS TV_Churn
FROM SO_FLAG
)
,ChurnedRGUs_SO_Prel AS(
SELECT DISTINCT *
,(VO_CHURN + BB_CHURN + TV_CHURN) AS RGUs_Prel
FROM RGUsFlag_SO
WHERE DxType='Voluntario'
)
,ChurnedRGUs_SO AS (
SELECT DISTINCT Month,Account_id,dxtype
,SUM(RGUs_Prel) AS ChurnedRGUs
FROM ChurnedRGUs_SO_Prel
GROUP BY 1,2,3
)
,RGUS_MixLastDay AS(
SELECT DISTINCT DATE_TRUNC('MONTH',DATE(load_dt)) AS Month,load_dt,FixedAccount,fi_outst_age
,CASE WHEN NumBB = 1 AND NumTV = 0 AND NumVO = 0 THEN 'BB'
    WHEN NumBB = 0 AND NumTV = 1 AND NumVO = 0 THEN 'TV'
    WHEN NumBB = 0 AND NumTV = 0 AND NumVO = 1 THEN 'VO'
    WHEN NumBB = 1 AND NumTV = 1 AND NumVO = 0 THEN 'BB+TV'
    WHEN NumBB = 1 AND NumTV = 0 AND NumVO = 1 THEN 'BB+VO'
    WHEN NumBB = 0 AND NumTV = 1 AND NumVO = 1 THEN 'VO+TV'
    WHEN NumBB = 1 AND NumTV = 1 AND NumVO = 1 THEN 'BB+VO+TV'
    WHEN PD_MIX_CD='0P' THEN '0P'
    END AS MixName_Adj
FROM FixedUsefulFields
)
,RGUSLastRecordDNA AS(
SELECT DISTINCT Month, FixedAccount
,first_value(mixname_adj) over(partition by FixedAccount,DATE_TRUNC('Month',load_dt) order by load_dt desc) as LastRGU
FROM RGUS_MixLastDay
WHERE (cast(fi_outst_age as double) <= 90 OR fi_outst_age IS NULL) 
)
,RGUSLastRecordDNA_Adj AS(
SELECT DISTINCT Month,FixedAccount,LastRGU
,CASE WHEN LastRGU IN ('VO', 'BB', 'TV') THEN 1
WHEN LastRGU IN ('BB+VO', 'BB+TV', 'VO+TV') THEN 2
WHEN lastRGU IN ('BB+VO+TV') THEN 3
WHEN lastRGU IN ('0P') THEN -1
ELSE 0 END AS NumRgusLastRecord
FROM RGUSLastRecordDNA
)
,LastRecordDateDNA AS(
SELECT DISTINCT DATE_TRUNC('MONTH',DATE(load_dt)) AS Month, 
FixedAccount,date(max(load_dt)) as LastDate
FROM FixedUsefulfields
WHERE (cast(fi_outst_age as double) <= 90 OR fi_outst_age IS NULL) 
GROUP BY 1, FixedAccount
)
,OverdueLastRecordDNA AS(
SELECT DISTINCT DATE_TRUNC('MONTH',DATE(load_dt)) AS Month, t.FixedAccount, fi_outst_age as LastOverdueRecord,(date_diff('day', DATE(MaxStart),DATE(load_dt))) as ChurnTenureDays
FROM FixedUsefulfields t 
INNER JOIN LastRecordDateDNA d ON t.FixedAccount = d.FixedAccount AND t.load_dt = d.LastDate
)
,VoluntaryFlag AS(
SELECT DISTINCT l.month,l.fixedaccount,dxtype,l.LastRGU,NumRgusLastRecord
,ChurnedRGUs
,CASE WHEN v.ChurnedRGUs >= l.NumRgusLastRecord THEN 1 ELSE 0 END AS Vol_Flag
FROM CHURNEDRGUS_SO v INNER JOIN RGUSLastRecordDNA_Adj l ON CAST(v.account_id AS VARCHAR)=l.fixedaccount AND v.month = l.Month
INNER JOIN LastRecordDateDNA d on l.fixedaccount=d.fixedaccount AND l.Month = date_trunc('month',d.lastdate)
INNER JOIN OverdueLastRecordDNA o ON l.fixedaccount = o.fixedaccount AND l.month = o.Month
)
,VoluntaryChurners AS(
SELECT DISTINCT s.FixedMonth,s.FixedAccount,F_ActiveBOM,F_ActiveEOM,B_Overdue,B_TechFlag,B_NumRGUs,B_MixName_Adj,B_MixCode_Adj,E_Overdue,E_TechFlag,E_NumRGUs,E_MixName_Adj,E_MixCode_Adj,FixedMainMovement,FixedSpinMovement,LastRGU,ChurnedRGUs,NumRgusLastRecord
,CASE WHEN v.FixedAccount IS NOT NULL and vol_flag=1  THEN 'Voluntario' END AS ChurnType
FROM SpinMovementBase s LEFT JOIN VoluntaryFlag v ON s.FixedAccount=v.FixedAccount AND s.FixedMonth=v.Month
)
,VoluntaryChurners_Adj AS(
SELECT DISTINCT FixedMonth AS Month,FixedAccount AS ChurnAccount,ChurnType
,CASE WHEN ChurnType IS NOT NULL AND F_ActiveEOM=1 AND B_NumRGUs>NumRgusLastRecord THEN 1 ELSE 0 END AS PartialChurn
FROM VoluntaryChurners
)
,FinalVoluntaryChurners AS(
SELECT DISTINCT MONTH, ChurnAccount
, CASE WHEN ChurnAccount IS NOT NULL THEN '1. Fixed Voluntary Churner' END AS FixedChurnerType
FROM VoluntaryChurners_Adj
WHERE ChurnType IS NOT NULL AND PartialChurn=0 
)
-- ######################################## Involuntary Churn #########################################################

,FIRSTCUSTRECORD AS (
    SELECT DATE_TRUNC('MONTH', Date_add('MONTH',1, DATE(LOAD_dt))) AS MES, FixedAccount AS Account, min(LOAD_dt) AS FirstCustRecord
    FROM FixedUsefulFields 
    WHERE LOAD_dt = date('2022-01-31')-- date_trunc('MONTH', DATE(LOAD_dt)) + interval '1' MONTH - interval '1' day
    Group by 1,2
)

,LastCustRecord as(
    SELECT  DATE_TRUNC('MONTH', DATE(LOAD_dt)) AS MES, FixedAccount AS Account, max(LOAD_dt) as LastCustRecord
    FROM FixedUsefulFields 
      --WHERE DATE(LOAD_dt) = date_trunc('MONTH', DATE(LOAD_dt)) + interval '1' MONTH - interval '1' day
   Group by 1,2
)
 ,NO_OVERDUE AS(
 SELECT DISTINCT DATE_TRUNC('MONTH', Date_add('MONTH',1, DATE(LOAD_dt))) AS MES, FixedAccount AS Account, fi_outst_age
 FROM FixedUsefulFields t
 INNER JOIN FIRSTCUSTRECORD  r ON t.LOAD_dt = r.FirstCustRecord and r.account = t.FixedAccount
 WHERE CAST(fi_outst_age as INT) <= 90
 GROUP BY 1, 2, 3
)
 ,OVERDUELASTDAY AS(
 SELECT DISTINCT DATE_TRUNC('MONTH', DATE(LOAD_dt)) AS MES, FixedAccount AS Account, fi_outst_age,
 (date_diff('DAY', DATE(LOAD_dt), MaxStart)) as ChurnTenureDays
 FROM FixedUsefulFields t
 INNER JOIN LastCustRecord r ON t.LOAD_dt = r.LastCustRecord and r.account = t.FixedAccount
 WHERE  CAST(fi_outst_age AS INTEGER) >= 90
 GROUP BY 1, 2, 3, 4
 )
 ,INVOLUNTARYNETCHURNERS AS(
 SELECT DISTINCT n.MES AS Month, n. account, l.ChurnTenureDays
 FROM NO_OVERDUE n INNER JOIN OVERDUELASTDAY l ON n.account = l.account and n.MES = l.MES
)
,InvoluntaryChurners AS(
SELECT DISTINCT Month, Account AS ChurnAccount, ChurnTenureDays
,CASE WHEN Account IS NOT NULL THEN '2. Fixed Involuntary Churner' END AS FixedChurnerType
FROM INVOLUNTARYNETCHURNERS 
GROUP BY 1, Account,4, ChurnTenureDays
)

,FinalInvoluntaryChurners AS(
    SELECT DISTINCT MONTH, ChurnAccount, FixedChurnerType
    FROM InvoluntaryChurners
    WHERE FixedChurnerType = '2. Fixed Involuntary Churner'
)
,AllChurners AS(
SELECT DISTINCT Month,ChurnAccount,FixedChurnerType
from (SELECT Month,ChurnAccount,FixedChurnerType from FinalVoluntaryChurners a 
      UNION ALL
      SELECT Month,ChurnAccount,FixedChurnerType  from FinalInvoluntaryChurners b)
WHERE Month=date('2022-02-01')
)
,FixedTable_ChurnFlag AS(
SELECT s.*,
CASE WHEN c.ChurnAccount IS NOT NULL THEN '1. Fixed Churner'
WHEN c.ChurnAccount IS NULL THEN '2. Fixed NonChurner'
END AS FixedChurnFlag,
CASE WHEN c.ChurnAccount is not null then FixedChurnerType END AS FixedChurnType
FROM SpinMovementBase s LEFT JOIN AllChurners c ON s.FixedAccount = c.ChurnAccount  and s.FixedMonth = c.Month
)
--################################## FMC AND Rejoiners ###########################################33
,Fixed_Convergency AS(
SELECT DISTINCT d.*, c.household_id
 ,CASE WHEN Tipo='1. Inscrito a Paquete completo' OR Tipo='2. Beneficio manual' THEN '1.Soft FMC'
       WHEN Tipo='2. Match_ID' OR Tipo='3. Contact number' THEN '2.Near FMC'
       WHEN E_Hard_FMC_Flag = 'Hard FMC' or B_Hard_FMC_Flag = 'Hard FMC' THEN '3. Hard FMC'
       ELSE '4.Fixed Only' END AS FMCFlagFix
FROM Convergente c RIGHT JOIN FixedTable_ChurnFlag d ON CAST(c.household_id AS VARCHAR(50))= CAST(d.FixedAccount AS VARCHAR(50))
  AND c.Mes=d.FixedMonth
)

,InactiveUsers AS (
SELECT DISTINCT FixedMonth AS ExitMonth, FixedAccount,DATE_ADD('MONTH',1,date(FixedMonth)) AS RejoinerMonth
FROM CustomerStatus
WHERE F_ActiveBOM=1 AND F_ActiveEOM=0
)
,RejoinerPopulation AS(
SELECT f.*,RejoinerMonth
,CASE WHEN i.FixedAccount IS NOT NULL THEN 1 ELSE 0 END AS RejoinerPopFlag
,CASE WHEN RejoinerMonth>=date('2022-02-01') AND RejoinerMonth<=DATE_ADD('MONTH',1,date('2022-02-01')) THEN 1 ELSE 0 END AS Fixed_PRMonth
FROM Fixed_Convergency f LEFT JOIN InactiveUsers i ON f.FixedAccount=i.FixedAccount AND FixedMonth=ExitMonth
)

,FixedRejoinerMonthPopulation AS(
SELECT DISTINCT FixedMonth,RejoinerPopFlag,Fixed_PRMonth,FixedAccount,date('2022-02-01') AS Month
FROM RejoinerPopulation
WHERE RejoinerPopFlag=1
AND Fixed_PRMonth=1
AND FixedMonth<>date('2022-02-01')
GROUP BY 1,2,3,4
)
,MonthFixedRejoiners AS(
SELECT f.*,Fixed_PRMonth
,CASE WHEN Fixed_PRMonth=1 AND FixedMainMovement='5.Come Back to Life'
THEN 1 ELSE 0 END AS Fixed_RejoinerMonth
FROM Fixed_COnvergency f LEFT JOIN FixedRejoinerMonthPopulation r ON f.FixedAccount=r.FixedAccount AND f.FixedMonth=CAST(r.Month AS DATE)
)
,InvFlagSO AS(
SELECT DISTINCT m.*
,CASE WHEN FixedChurnFlag='2. Fixed NonChurner' AND F_ActiveEOM=0 THEN 'Churner Gap' END AS Gap
,CASE WHEN DXtype='Involuntario' AND FixedChurnFlag='2. Fixed NonChurner' AND F_ActiveEOM=0 THEN 1 ELSE 0 END AS earlydxflag
,CASE WHEN DXtype='Migracion' AND FixedChurnFlag='2. Fixed NonChurner' AND F_ActiveEOM=0 THEN 1 ELSE 0 END AS migrtflag
FROM MonthFixedRejoiners m LEFT JOIN SO_Flag s ON m.FixedAccount=CAST(s.Account_id AS VARCHAR) AND DATE_TRUNC('MONTH',s.StartDate)=m.FixedMonth
)
,prepaidchurners as(
select distinct date(date_trunc('month',load_dt)) as month,date(load_dt) as date, act_acct_cd
FROM "lla_cco_int_stg"."cwp_fix_union_dna"
WHERE act_cust_typ_nm = 'Residencial' and pd_mix_cd='0P'
)
,PrepaidChurnerflag AS(
SELECT DISTINCT f.*
,CASE WHEN b_mixcode_adj IS NOT NULL and r.act_acct_cd IS NOT NULL THEN 'Churner0P' end as churn0p
FROM InvFlagSO f LEFT JOIN prepaidchurners r ON f.fixedmonth=r.month and f.fixedaccount=r.act_acct_cd
)
,FinalChurnFlag_SO AS(
SELECT DISTINCT FixedMonth,FixedAccount,F_ActiveBOM,F_ActiveEOM,Fix_B_Date,FixedAccount_BOM,Fixed_B_Phone,B_Overdue,Fixed_B_MaxStart,B_FixedTenure,B_Fixed_MRC,B_TechFlag,B_NumRGUs,B_MixName_Adj,B_MixCode_Adj,B_BB,B_TV,B_VO,B_bbCode,B_tvCode,B_voCode,B_Hard_FMC_Flag,Fix_E_Date,Fixed_E_Phone,E_Overdue,Fixed_E_MaxStart,E_FixedTenure,E_Fixed_MRC,E_TechFlag,E_NumRGUs,E_MixName_Adj,E_MixCode_Adj,E_BB,E_TV,E_VO,E_bbCode,E_tvCode,E_voCode,E_Hard_FMC_Flag,First_sales_chnl_bom,Last_sales_chnl_bom,First_sales_chnl_eom,Last_sales_chnl_eom,FixedMainMovement,FixedSpinMovement
,CASE WHEN  (earlydxflag=1 OR migrtflag=1) THEN '1. Fixed Churner'
      WHEN FixedChurnFlag='2. Fixed NonChurner' and f_activebom=1 and f_activeeom=0 AND churn0p='Churner0P' THEN '1. Fixed Churner'
      ELSE FixedChurnFlag
END AS FixedChurnFlag
,CASE WHEN earlydxflag=1 THEN '2. Fixed Involuntary Churner'
      WHEN migrtflag=1 THEN '1. Fixed Voluntary Churner'
      WHEN FixedChurnFlag='2. Fixed NonChurner' and f_activebom=1 and f_activeeom=0 AND churn0p='Churner0P' THEN '3. Fixed 0P Churner'
      ELSE FixedChurnType 
END AS FixedChurnType
,CASE WHEN earlydxflag=1 THEN 'Early Dx'
      WHEN migrtflag=1 THEN 'Incomplete CST'
      WHEN FixedChurnType='1. Fixed Voluntary Churner' THEN 'Voluntary'
      WHEN FixedChurnType='2. Fixed Involuntary Churner' THEN 'Involuntary'
      WHEN FixedChurnFlag='2. Fixed NonChurner' and f_activebom=1 and f_activeeom=0 AND churn0p='Churner0P' THEN '0P Churner'
END AS FixedChurnSubtype
,household_id,FMCFlagFix,Fixed_PRMonth,Fixed_RejoinerMonth,gap
FROM PrepaidChurnerFlag
)
SELECT DISTINCT *
FROM FinalChurnFlag_SO
