with
ejercicio1 as(
SELECT distinct date_trunc('month',date(dt)) as month,act_acct_cd
FROM "db-analytics-prod"."fixed_cwp" 
where act_cust_typ_nm = 'Residencial' -- filtro para clientes residenciales
and date(dt)=date_trunc('month',date(dt)) --filtro para tomar el 1er día de cada mes
and year(date(dt))=2022 -- filtro para extraer únicamente los datos del 2022
and pd_bb_prod_cd is not null -- filtro para extraer únicamente los usuarios con BB
)
,interactions as(
select distinct date_trunc('month',date(INTERACTION_START_TIME)) as month,account_id
from "db-stage-prod"."interactions_cwp"
where interaction_purpose_descrip='TRUCKROLL' --filtro para especificar usuarios con truck rolls
)
select distinct a.month,count(distinct a.act_acct_cd) as users_with_truckrolls
from ejercicio1 a inner join interactions b on a.act_acct_cd=b.account_id and a.month=b.month
group by 1 order by 1
