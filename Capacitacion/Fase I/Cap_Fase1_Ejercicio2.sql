select distinct date_trunc('month',date(order_start_date)) as month,count(distinct order_id) as orders
FROM "db-stage-dev"."so_hdr_cwc"
where org_cntry = 'Jamaica' and account_type = 'Residential' -- filtros para clientes residenciales de Jamaica
and year(date(order_start_date))=2022 -- filtro para extraer únicamente los datos del 2022
group by 1 order by 1
