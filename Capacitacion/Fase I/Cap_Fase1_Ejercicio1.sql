SELECT distinct date_trunc('month',date(dt)) as month,count(distinct act_acct_cd)
FROM "db-analytics-prod"."fixed_cwp" 
where act_cust_typ_nm = 'Residencial' -- filtro para clientes residenciales
and date(dt)=date_trunc('month',date(dt)) --filtro para tomar el 1er día de cada mes
and year(date(dt))=2022 -- filtro para extraer únicamente los datos del 2022
group by 1 order by 1
