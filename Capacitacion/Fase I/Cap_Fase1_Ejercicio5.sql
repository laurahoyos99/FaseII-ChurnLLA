with usuarios as(
select distinct date(dt) as date, account_id
,date_diff('DAY',  cast (concat(substr(oldest_unpaid_bill_dt, 1,4),'-',substr(oldest_unpaid_bill_dt, 5,2),'-', substr(oldest_unpaid_bill_dt, 7,2)) as date), cast(dt as date)) as fi_outst_age -- cálculo de los días de mora
FROM "db-analytics-prod"."tbl_postpaid_cwc" 
WHERE org_id = '338' AND account_type ='Residential' -- filtros para usuarios residenciales de jamaica
and date(dt)=date('2022-10-01') -- filtro para evaluar únicamente el 1 de octubre
)
select distinct date
,case when fi_outst_age>=90 then 'Inactive'
      when fi_outst_age<90 or fi_outst_age is null then 'Active'
else null end as  Active_Flag -- creacion de las categorias "Inactive" y "Active" en base a la columna de mora
,count(distinct account_id) as users
from usuarios
group by 1,2 order by 1,2
