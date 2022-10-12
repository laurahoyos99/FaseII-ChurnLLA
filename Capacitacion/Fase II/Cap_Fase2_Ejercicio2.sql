with usuarios as(
select distinct date(dt) as date, account_id
,date_diff('DAY',  cast (concat(substr(oldest_unpaid_bill_dt, 1,4),'-',substr(oldest_unpaid_bill_dt, 5,2),'-', substr(oldest_unpaid_bill_dt, 7,2)) as date), cast(dt as date)) as fi_outst_age -- cálculo de los días de mora
FROM "db-analytics-prod"."tbl_postpaid_cwc" 
WHERE org_id = '338' AND account_type ='Residential' -- filtros para usuarios residenciales de jamaica
and date(dt)=date('2022-10-01') -- filtro para evaluar únicamente el 1 de octubre
)
,ejercicio5 as(
select distinct date_trunc('month',date) as month
,case when fi_outst_age>=90 then 'Inactive'
      when fi_outst_age<90 or fi_outst_age is null then 'Active'
else null end as  Active_Flag -- creacion de las categorias "Inactive" y "Active" en base a la columna de mora
,account_id
from usuarios
)
,dx_orders as(
select distinct date_trunc('month',date(order_start_date)) as month,account_id
FROM "db-stage-dev"."so_hdr_cwc"
where org_cntry = 'Jamaica' and account_type = 'Residential' -- filtros para clientes residenciales de Jamaica
and year(date(order_start_date))=2022 -- filtro para extraer únicamente los datos del 2022
and order_type = 'DEACTIVATION' -- filtro para órdenes de desconexión
)
select distinct a.month,count(distinct a.account_id) as users_with_dx_orders
from ejercicio5 a inner join dx_orders b on a.account_id=cast(b.account_id as varchar) and a.month=b.month
where Active_Flag='Active' -- filtro para extraer únicamente los usuarios clasificados como activos
group by 1 order by 1
