with
ejercicio1 as(
SELECT distinct date_trunc('month',date(dt)) as month,act_acct_cd
FROM "db-analytics-prod"."fixed_cwp" 
where act_cust_typ_nm = 'Residencial' -- filtro para clientes residenciales
and year(date(dt))=2022 -- filtro para extraer únicamente los datos del 2022
and (fi_outst_age<90 or fi_outst_age is null) -- filtro para usuarios sin morosidad
)
,interactions as(
select distinct date_trunc('month',date(INTERACTION_START_TIME)) as month,account_id,interaction_id
from "db-stage-prod"."interactions_cwp"
where interaction_purpose_descrip='CLAIM' -- filtro para considerar únicamente reclamos
)
,interactions_per_user as(
select distinct a.month,act_acct_cd,count(distinct interaction_id) as interactions 
-- se agrupan las llamadas por mes y contrato para saber cuantás llamadas tiene cada cliente por mes
from ejercicio1 a inner join interactions b on a.act_acct_cd=b.account_id and a.month=b.month
group by 1,2 order by 1,2
)
,categories as(
select distinct *
,case when interactions=1 then 'Usuarios = 1 llamada'
      when interactions>1 then 'Usuarios > 1 llamada'
else null end as Interactions_Tier --Creación de las categorías
from interactions_per_user
)
select distinct month,interactions_tier,count(distinct act_acct_cd) as users
from categories
group by 1,2 order by 1,2
