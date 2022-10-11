with
ejercicio1 as(
SELECT distinct date_trunc('month',date(dt)) as month,act_acct_cd
,date(dt) as extraction_date,date(act_cust_strt_dt) as Customer_Start_Date 
,case when date_diff('day',date(act_cust_strt_dt),date(dt))<=180 then '1. Early Tenure'
      when date_diff('day',date(act_cust_strt_dt),date(dt)) between 181 and 359 then '2. Mid Tenure'
      when date_diff('day',date(act_cust_strt_dt),date(dt))>=360 then '3. Late Tenure'
else null end as Tenure --Se calcula el tenure como la fecha de extracción - la fecha de inicio del cliente
FROM "db-analytics-prod"."fixed_cwp" 
where act_cust_typ_nm = 'Residencial' -- filtro para clientes residenciales
and date(dt)=date_trunc('month',date(dt)) --filtro para tomar el 1er día de cada mes
and year(date(dt))=2022 -- filtro para extraer únicamente los datos del 2022
)

select distinct month,tenure,count(distinct act_acct_cd) as accounts
from ejercicio1
group by 1,2 order by 1,2
--Habrán usuarios con tenure en null ya que no tienen fecha de inicio
