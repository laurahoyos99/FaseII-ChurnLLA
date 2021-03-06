WITH SELECTED_MONTHS AS (
SELECT DISTINCT DATE_TRUNC(FECHA_EXTRACCION, MONTH) AS MONTH
FROM `gcp-bia-tmps-vtr-dev-01.gcp_temp_cr_dev_01.2022-04-20_Historical_CRM_ene_2021_mar_2022_D` 
WHERE DATE_TRUNC(FECHA_EXTRACCION, MONTH) IN ('2021-07-01','2021-08-01','2021-09-01')
)
, MONTH_ORDER AS (
SELECT MONTH, ROW_NUMBER() OVER (ORDER BY MONTH) AS MONTH_ORDER_ROW
FROM SELECTED_MONTHS
)
, CHURN_FILTER AS (
SELECT DISTINCT ACT_ACCT_CD, DATE(DATE_TRUNC(MAX(CST_CHRN_DT), MONTH)) AS CHURN_DATE
FROM `gcp-bia-tmps-vtr-dev-01.gcp_temp_cr_dev_01.2022-04-20_Historical_CRM_ene_2021_mar_2022_D` 
GROUP BY ACT_ACCT_CD
HAVING EXTRACT (MONTH FROM CHURN_DATE) = EXTRACT (MONTH FROM MAX(FECHA_EXTRACCION))
)
, CHURN_MONTH_FILTER AS (
SELECT *
FROM CHURN_FILTER CF
WHERE CF.CHURN_DATE BETWEEN ((SELECT MAX(MONTH) FROM MONTH_ORDER)) AND DATE_ADD((SELECT MAX(MONTH) FROM MONTH_ORDER), INTERVAL 3 MONTH)
)
, MONTH_LAST AS (
SELECT DATE_TRUNC(FECHA_EXTRACCION, MONTH) MONTH
    , MIN(FECHA_EXTRACCION) LAST_DATE_MONTH
FROM `gcp-bia-tmps-vtr-dev-01.gcp_temp_cr_dev_01.2022-04-20_Historical_CRM_ene_2021_mar_2022_D` 
WHERE DATE_TRUNC(FECHA_EXTRACCION, MONTH) BETWEEN ((SELECT MIN(MONTH) FROM MONTH_ORDER)) AND DATE_ADD((SELECT MAX(MONTH) FROM MONTH_ORDER), INTERVAL 3 MONTH)
GROUP BY 1
)
, BASE AS (
SELECT M.MONTH_ORDER_ROW
    , CRM.ACT_ACCT_CD
    , ROUND((CRM.VO_FI_TOT_MRC_AMT - CRM.VO_FI_TOT_MRC_AMT_DESC) + (CRM.BB_FI_TOT_MRC_AMT - CRM.BB_FI_TOT_MRC_AMT_DESC) + (CRM.TV_FI_TOT_MRC_AMT - CRM.TV_FI_TOT_MRC_AMT_DESC),2) AS MRC_TOTAL
    , ROUND((TOT_BILL_AMT-TOT_DESC_AMT),2) AS BILLING_TOTAL
FROM `gcp-bia-tmps-vtr-dev-01.gcp_temp_cr_dev_01.2022-04-20_Historical_CRM_ene_2021_mar_2022_D`  CRM
    INNER JOIN MONTH_LAST ML
        ON CRM.FECHA_EXTRACCION = ML.LAST_DATE_MONTH
    INNER JOIN MONTH_ORDER M
        ON M.MONTH = ML.MONTH
)
, PIVOT_VALS AS (
SELECT ACT_ACCT_CD
    , SUM(CASE WHEN MONTH_ORDER_ROW = 1 THEN MRC_TOTAL END) AS MRC_MONTH_1
    , SUM(CASE WHEN MONTH_ORDER_ROW = 2 THEN MRC_TOTAL END) AS MRC_MONTH_2
    , SUM(CASE WHEN MONTH_ORDER_ROW = 3 THEN MRC_TOTAL END) AS MRC_MONTH_3 
    , SUM(CASE WHEN MONTH_ORDER_ROW = 1 THEN BILLING_TOTAL END) AS BILL_MONTH_1
    , SUM(CASE WHEN MONTH_ORDER_ROW = 2 THEN BILLING_TOTAL END) AS BILL_MONTH_2
    , SUM(CASE WHEN MONTH_ORDER_ROW = 3 THEN BILLING_TOTAL END) AS BILL_MONTH_3 
FROM BASE 
GROUP BY 1
)
, CONSULTAFINAL AS(
SELECT PV.*
    , CASE 
        WHEN MRC_MONTH_1 IS NULL OR MRC_MONTH_2 IS NULL THEN NULL
        WHEN MRC_MONTH_1 - MRC_MONTH_2 >= 1300 THEN 'WENT DOWN'
        WHEN MRC_MONTH_2 - MRC_MONTH_1 >= 1300 THEN 'WENT UP'
        ELSE 'SAME'
    END AS MONTH2_VS_MONTH1
    , MRC_MONTH_2/NULLIF(MRC_MONTH_1,0) - 1 AS CHANGE_PERC_2_VS_1 
    , CASE 
        WHEN MRC_MONTH_2 IS NULL OR MRC_MONTH_3 IS NULL THEN NULL
        WHEN MRC_MONTH_2 - MRC_MONTH_3 >= 1300 THEN 'WENT DOWN'
        WHEN MRC_MONTH_3 - MRC_MONTH_2 >= 1300 THEN 'WENT UP'
        ELSE 'SAME'
    END AS MONTH3_VS_MONTH2
    , MRC_MONTH_3/NULLIF(MRC_MONTH_2,0) - 1 AS CHANGE_PERC_3_VS_2
    , CASE 
        WHEN BILL_MONTH_1 IS NULL OR BILL_MONTH_2 IS NULL THEN NULL
        WHEN BILL_MONTH_1 - BILL_MONTH_2 >=1300 THEN 'WENT DOWN'
        WHEN BILL_MONTH_2 - BILL_MONTH_1 >=1300 THEN 'WENT UP'
        ELSE 'SAME'
    END AS MONTH2_VS_MONTH1_BILL
    , BILL_MONTH_2/NULLIF(BILL_MONTH_1,0) - 1 AS CHANGE_PERC_2_VS_1_BILL 
    , CASE 
        WHEN BILL_MONTH_2 IS NULL OR BILL_MONTH_3 IS NULL THEN NULL
        WHEN BILL_MONTH_2 - BILL_MONTH_3 >=1300 THEN 'WENT DOWN'
        WHEN BILL_MONTH_3 - BILL_MONTH_2 >=1300 THEN 'WENT UP'
        ELSE 'SAME'
    END AS MONTH3_VS_MONTH2_BILL
    , BILL_MONTH_3/NULLIF(BILL_MONTH_2,0) - 1 AS CHANGE_PERC_3_VS_2_BILL
FROM PIVOT_VALS PV
)
SELECT *
FROM CONSULTAFINAL

