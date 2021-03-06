WITH 
UsefulFieldsMobile AS(
SELECT SAFE_CAST(dt AS DATE) AS DT, DATE_TRUNC(SAFE_CAST(dt AS DATE),Month) AS MonthM
,LEFT(CONCAT(ACCOUNTNO,'000000000000') ,12) AS ACT_ACCT_CDM
,SAFE_CAST(SERVICENO AS INT64) AS SERVICENO
,MAX(SAFE_CAST(PARSE_DATETIME('%Y.%m.%d %H:%M:%S',STARTDATE_ACCOUNTNO) AS DATE)) AS MaxStart
,ACCOUNTNAME,NUMERO_IDENTIFICACION,SAFE_CAST(TOTAL_MRC_D AS FLOAT64) AS mrc_amt
,INV_PAYMT_DT
FROM `gcp-bia-tmps-vtr-dev-01.lla_temp_dna_tables.cwp_info_dna_postpaid_history` 
WHERE BIZ_UNIT_D="B2C" AND ACCOUNT_STATUS IN ('ACTIVE','GROSS_ADDS','PORT_IN') AND INV_PAYMT_DT<>"nan"
GROUP BY DT,MonthM,ACT_ACCT_CDM
,SERVICENO,ACCOUNTNAME,NUMERO_IDENTIFICACION,mrc_amt,INV_PAYMT_DT
)
,UsefulFieldsFixed AS(
SELECT ACT_ACCT_CD, DT,act_cust_strt_dt, PD_VO_PROD_CD, PD_BB_PROD_CD, PD_TV_PROD_CD,
avg(FI_VO_MRC_AMT) AS avgVO, avg(FI_BB_MRC_AMT) as avgBB, avg(FI_TV_MRC_AMT) AS avgTV
,SAFE_CAST(ACT_CONTACT_PHONE_1 AS INT64) AS ACT_CONTACT_PHONE_1,ACT_ACCT_ID_VAL
,DATE_TRUNC(DATE_SUB(DT, INTERVAL 1 MONTH),MONTH) AS MONTH
FROM `gcp-bia-tmps-vtr-dev-01.lla_temp_dna_tables.cwp_info_dna_fixed_history_v2` 
  WHERE PD_MIX_CD<>"0P"
  AND (safe_cast(fi_outst_age as int64) <= 90 OR fi_outst_age IS NULL)
  AND dt='2022-03-02'
  GROUP BY 1,2,3,4,5,6,ACT_CONTACT_PHONE_1,ACT_ACCT_ID_VAL
)
,PaqueteCompleto AS(
SELECT *,DATE_TRUNC(PARSE_DATE("%Y%m%d",Date),MONTH) as Mes
FROM `gcp-bia-tmps-vtr-dev-01.lla_temp_dna_tables.20220330_cwp_reporte_fmc` 
)
,UsuariosFMC1 AS(
SELECT ACT_CONTACT_PHONE_1,ACT_ACCT_ID_VAL
,CASE WHEN ACT_ACCT_CD IS NOT NULL AND ACT_ACCT_CD IS NOT NULL THEN ACT_ACCT_CD END AS ACCOUNT
,CASE WHEN Month IS NOT NULL AND Month IS NOT NULL THEN Month END AS MonthA
FROM(
SELECT DISTINCT *,ACT_ACCT_CD AS ACT_ACCT_CD1,Month as Month1
FROM UsefulFieldsFixed f
       INNER JOIN UsefulFieldsMobile m ON f.ACT_CONTACT_PHONE_1=m.SERVICENO AND f.Month=m.MonthM
UNION ALL
SELECT DISTINCT *,ACT_ACCT_CD AS ACT_ACCT_CD2,Month as Month2
FROM UsefulFieldsFixed f
     INNER JOIN UsefulFieldsMobile m ON f.ACT_ACCT_ID_VAL=m.NUMERO_IDENTIFICACION AND f.Month=m.MonthM
)
)
,UsuariosFMC2 AS(
SELECT DISTINCT MonthM,SERVICENO,ACT_ACCT_CDM
FROM PaqueteCompleto p INNER JOIN UsefulFieldsMobile m ON SERVICE_ID=SERVICENO and mes=monthM
)
,BaseFlagFMC AS(
SELECT u.* 
,CASE WHEN a.ACCOUNT IS NULL AND b.ACT_ACCT_CDM IS NOT NULL THEN 1 
      WHEN b.ACT_ACCT_CDM IS NULL AND a.ACCOUNT IS NOT NULL THEN 1
      ELSE 0 END AS FlagFMC
FROM UsefulfieldsFixed u LEFT JOIN UsuariosFMC1 a ON u.ACT_ACCT_CD=a.Account AND u.Month=a.MonthA
 LEFT JOIN UsuariosFMC2 b ON u.ACT_CONTACT_PHONE_1=b.SERVICENO AND u.Month=b.MonthM
)
SELECT DISTINCT Month, FlagFMC, COUNT(DISTINCT ACT_ACCT_CD) AS Records
FROM BaseFlagFMC 
GROUP BY Month, FlagFMC
ORDER BY Month, FlagFMC

--select * from usuariosfmc1
