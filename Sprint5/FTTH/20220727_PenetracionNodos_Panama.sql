with
FMC_Table AS ( 
SELECT * FROM "lla_cco_int_ana_prod"."cwp_fmc_churn_prod"
where month=date(dt)
)
,ftth_accounts_month_adj as(
select distinct case month
                     when 20220201 then date('2022-02-01')
                     when 20220301 then date('2022-03-01')
                     when 20220401 then date('2022-04-01')
                     when 20220501 then date('2022-05-01')
                end as Month_Adj
,trim(building_fdh) as trim_fdh,*
FROM "lla_cco_int_san"."cwp_con_ext_ftth_ad"
)
,join_dna as(
select distinct p.*
from fmc_table f inner join ftth_accounts_month_adj p on f.finalaccount=cast(p.acct_no as varchar) and f.month=p.month_adj
)
,coordenates as(
SELECT distinct provincia,distrito,case when distrito='Panama' and corregimiento like '%anitas%' then 'Las Mañanitas' when corregimiento='Betania' then 'Bethania' when (distrito='Panama' and corregimiento='Parque Lefebre') then 'Parque Lefevre' else corregimiento end as corregimiento,longitude,latitude
FROM "lla_cco_int_san"."cwp_ext_corregimientos_ftth" 
)
,ftth_project as(
select *, trim(b."nodo/fdh") as nodo,Tipo as Tech,b."home passed" as Home_Passed,b."velocidad máxima (coaxial)" as velocidad
,REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(lower(provincia), 'á', 'a'), 'é','e'), 'í', 'i'), 'ó', 'o'), 'ú','u') as provincias
,REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(lower(DISTRITO), 'á', 'a'), 'é','e'), 'í', 'i'), 'ó', 'o'), 'ú','u') as distritos
,REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(lower(corregimiento), 'á', 'a'), 'é','e'), 'í', 'i'), 'ó', 'o'), 'ú','u') as corregimientos
FROM "lla_cco_int_san"."cwp_ext_ftth" b
where provincia='Panamá' and b."velocidad máxima (coaxial)"=1000
)
,count_districts as(
select distinct dt,provincias,distritos,nodo,count(distinct corregimientos) as cont
from ftth_project
group by 1,2,3,4 order by 1,4,cont desc
)
,ftth_project_adj as(
select distinct *,first_value(corregimientos) over(partition by dt,nodo order by home_passed desc) as corregimiento_adj
from(select distinct f.dt,f.provincias,f.distritos,f.corregimientos,f.nodo,cont,sum(f.home_passed) as home_passed
from ftth_project f left join count_districts c on f.dt=c.dt and f.provincias=c.provincias and f.nodo=c.nodo
--where velocidad=1000 --and cont>1
group by 1,2,3,4,5,6) order by 1,5
)
,ftth_join_coord as (
select distinct dt,provincia,distrito,corregimiento,nodo,home_passed,longitude,latitude
from ftth_project_adj f left join coordenates c on f.provincias=lower(c.provincia) and f.distritos=lower(c.distrito) and f.corregimiento_adj=lower(c.corregimiento)
)
,penetration_fields as(
select distinct date(b.dt) as Month,Provincia,Distrito,Corregimiento,acct_no,nodo,sum(home_passed) as home_passed,longitude,latitude
from ftth_join_coord b left join join_dna a on b.nodo=a.trim_fdh and date(b.dt)=month_adj
--where velocidad=1000
group by 1,2,3,4,5,6,longitude,latitude
)
,initial_grouping as(
select distinct Month,Provincia,Distrito,Corregimiento,longitude,latitude,Nodo,Home_Passed,count(distinct acct_no) as Active_Users
from penetration_fields
--where month=date('2022-05-01') and nodo='ABG-001'
group by 1,2,3,4,5,6,7,8
)
,final as(
select distinct Month,Provincia,Distrito,Corregimiento,longitude,latitude,Nodo,sum(Home_Passed) as Home_Passed,Active_Users,Active_Users*100/sum(home_passed) as P
from initial_grouping
where month=date('2022-05-01') --and nodo='NSG-001'
group by 1,2,3,4,5,6,7,active_users
order by 1,nodo,2,3,4
)
select distinct * 
from final
