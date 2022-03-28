WITH 

UsefulFields AS(
SELECT account_id,dt
FROM `gcp-bia-tmps-vtr-dev-01.lla_temp_dna_tables.cwc_info_dna_postpaid_history_v2` 
WHERE org_id = "338" AND account_type ="Residential"
AND account_status NOT IN('Ceased','Closed','Recommended for cease')
)
,ActiveUsersBOM AS(
SELECT DISTINCT DATE_TRUNC(DATE_ADD(dt, INTERVAL 1 MONTH),MONTH) AS Month, account_id AS accountBOM,dt,
FROM UsefulFields
WHERE dt=LAST_DAY(dt, MONTH)
GROUP BY 1,2,3
)
,ActiveUsersEOM AS(
SELECT DISTINCT DATE_TRUNC(DATE(dt),MONTH) AS Month, account_id AS accountEOM,dt
FROM UsefulFields
WHERE dt= LAST_DAY(DATE(dt), MONTH)
GROUP BY 1,2,3
)
,CustomerStatus AS(
  SELECT
   CASE WHEN (accountBOM IS NOT NULL AND accountEOM IS NOT NULL) OR (accountBOM IS NOT NULL AND accountEOM IS NULL) THEN b.Month
      WHEN (accountBOM IS NULL AND accountEOM IS NOT NULL) THEN e.Month
  END AS Month,
      CASE WHEN (accountBOM IS NOT NULL AND accountEOM IS NOT NULL) OR (accountBOM IS NOT NULL AND accountEOM IS NULL) THEN accountBOM
      WHEN (accountBOM IS NULL AND accountEOM IS NOT NULL) THEN accountEOM
  END AS account_id,
  CASE WHEN accountBOM IS NOT NULL THEN 1 ELSE 0 END AS ActiveBOM,
  CASE WHEN accountEOM IS NOT NULL THEN 1 ELSE 0 END AS ActiveEOM,
  FROM ActiveUsersBOM b FULL OUTER JOIN ActiveUsersEOM e
  ON b.accountBOM = e.accountEOM AND b.MONTH = e.MONTH
)
,PotentialRejoiner AS(
SELECT *
,CASE WHEN ActiveBOM=1 AND ActiveEOM=0 THEN DATE_ADD(Month, INTERVAL 4 MONTH) ELSE NULL END AS PR
FROM CustomerStatus
ORDER BY account_id,Month
)
,PotentialRejoinersFeb AS(
SELECT *
,CASE WHEN PR>='2022-02-01' AND PR<=DATE_ADD('2022-02-01',INTERVAL 4 MONTH) THEN 1 ELSE 0 END AS PRFeb
FROM PotentialRejoiner
),
POTREJOINERS AS(
SELECT DISTINCT account_id  FROM PotentialRejoinersFeb 
WHERE PRfeb=1 
)
--COUNTREJOINERPOP AS (
    SELECT COUNT(*)
    FROM POTREJOINERS AS RejoinerPopulation
