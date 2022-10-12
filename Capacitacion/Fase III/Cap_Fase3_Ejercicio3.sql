with
ejercicio1 as(
SELECT distinct date_trunc('month',date(dt)) as month,act_acct_cd
FROM "db-analytics-prod"."fixed_cwp" 
where act_cust_typ_nm = 'Residencial' -- filtro para clientes residenciales
and date(dt)=date_trunc('month',date(dt)) --filtro para tomar el 1er día de cada mes
and year(date(dt))=2022 -- filtro para extraer únicamente los datos del 2022
and (fi_outst_age<90 or fi_outst_age is null) -- filtro para usuarios sin morosidad
)
,interactions as(
select distinct date_trunc('month',date(INTERACTION_START_TIME)) as month,date(INTERACTION_START_TIME) as interaction_date,account_id
from "db-stage-prod"."interactions_cwp" 
)
,service_orders as(
select distinct date_trunc('month',date(order_start_date)) as month,date(order_start_date) as order_date,account_id
from "db-stage-dev"."so_hdr_cwp" 
where order_type = 'DEACTIVATION'
)
select distinct a.month,count(distinct act_acct_cd) as users
from ejercicio1 a 
 inner join interactions b on a.act_acct_cd=b.account_id and a.month=b.month -- asegura que la interaccion se realiza el mismo mes en el que el usuario está activo en el DNA
 inner join service_orders c on a.act_acct_cd=cast(c.account_id as varchar) --union solo por cuenta porque la orden se pudo haber realizado en el mes siguiente
where order_date>interaction_date and date_diff('day',interaction_date,order_date)<=40 --asegura que la orden se realizo después de la interacción y que entre ambas no pasarón más de 40 días
group by 1 order by 1
