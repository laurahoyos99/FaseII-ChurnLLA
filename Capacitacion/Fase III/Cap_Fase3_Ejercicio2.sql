with
active_users as(
SELECT distinct date_trunc('month',date(dt)) as month,date(dt) as date,act_acct_cd
FROM "db-analytics-prod"."fixed_cwp" 
where act_cust_typ_nm = 'Residencial' -- filtro para clientes residenciales
and year(date(dt))=2022 -- filtro para extraer únicamente los datos del 2022
and (fi_outst_age<90 or fi_outst_age is null) -- filtro para usuarois sin morosidad
)
,first_value_last_day as(
select distinct *
,first_value(date) over(partition by act_acct_cd,month order by date desc) as Last_DNA_Date
--encuentra la última fecha en la que aparece cada contrato cada mes
from active_users
)
select distinct month,Last_DNA_Date,count(distinct act_acct_cd) as Users
from first_value_last_day
group by 1,2 order by 1,2
