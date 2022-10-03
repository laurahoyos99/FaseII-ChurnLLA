with
--------------------------------Input Tables-------------------------------------------------------
fmc_table_network_cwp as(
select distinct month
,case when tech_concat LIKE '%FTTH%' then 'FTTH' when tech_concat NOT LIKE '%FTTH%' and tech_concat LIKE '%HFC%' then 'HFC' when tech_concat NOT LIKE '%FTTH%' and tech_concat NOT LIKE '%HFC%' AND tech_concat LIKE '%COPPER%' THEN 'COPPER' when tech_concat NOT LIKE '%FTTH%' and tech_concat NOT LIKE '%HFC%' AND tech_concat NOT LIKE '%COPPER%' AND tech_concat LIKE '%Wireless%' then 'Wireless' else null end as Tech
--,count(distinct churners) as churners
from(select distinct month
,concat(coalesce(b_final_techflag,''),coalesce(e_final_techflag,'')) as tech_concat
,fixedaccount
--,case when fixedchurntype is not null then fixedaccount  --cwp
--,case when fixedchurntype (nombre cwj) and f_activeeom=0 is not null then fixedaccount 
--CREATE TABLE IF NOT EXISTS "lla_cco_int_stg"."cr_fix_stg_dashboardinput_dinamico_Prueba2SEPT_jul" AS

--validar jamaica porque bandera de churntype tambien tiene downsells
)
from "lla_cco_int_ana_prod"."cwp_fmc_churn_prod" 
)
,fmc_table_month_cwp as(
select distinct month,'OVERALL' as Network,sum(churners) as churners
from fmc_table_netw
)

--para costa rica yo me esperaria a que este check el sprint 1&2 de costa rica recomendacion personal
--crear una tabla con el sprint 1&2 para poder jalarla acá

,FIXED_DATA AS(
SELECT distinct date_trunc('month',cast(date_parse(month,'%m/%d/%y') as date)) as month,market,network,
        cast(CASE WHEN "total subscribers"  = 'nan' then null else "total subscribers" end as double) as total_subscribers,
        cast(CASE WHEN "assisted installations"  = 'nan' then null else "assisted installations" end as double) as assisted_instalations,
        cast(CASE WHEN mtti  = 'nan' then null else mtti end as double) as mtti,
        cast(CASE WHEN "truck rolls"  = 'nan' then null else "truck rolls" end as double) as truck_rolls,
        cast(CASE WHEN mttr  = 'nan' then null else mttr end as double) as mttr,
        cast(CASE WHEN scr  = 'nan' then null else scr end as double) as scr,
        cast(CASE WHEN "i-elf(28days)"  = 'nan' then null else "i-elf(28days)" end as double) as i_elf_28days,
        cast(CASE WHEN "r-elf(28days)"  = 'nan' then null else "r-elf(28days)" end as double) as r_elf_28days,
        cast(CASE WHEN "i-sl"  = 'nan' then null else "i-sl" end as double) as i_sl,
        cast(CASE WHEN "r-sl"  = 'nan' then null else "r-sl" end as double) as r_sl
FROM "lla_cco_int_san"."cwp_ext_servicedelivery_monthly"
)

,service_delivery as(
SELECT  distinct month as Month,Network,market,round(assisted_instalations,0) as Install, round(mtti,2) as MTTI,round(truck_rolls,0) as Repairs,round(mttr,2) as MTTR,round(scr,2) as Repairs_1k_rgu,round((100-i_elf_28days)/100,4) as FTR_Install, round((100-r_elf_28days)/100,4) as FTR_Repair,round((i_sl/assisted_instalations),4) as Installs_SL,round((r_sl/truck_rolls),4) as Repairs_SL, round(i_sl,0) as Inst_SL,round(r_sl,0) as Rep_SL
FROM FIXED_DATA
WHERE market in('Panama','Jamaica','Puerto Rico', 'Costa Rica') 
--GROUP BY 1,2--,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18
ORDER BY 1,2--,3
)
,pnps_kpi as(
select distinct date(date_parse(cast(month as varchar),'%Y%m%d')) as month,Opco,case when opco='CT' then 'Costa Rica' when opco='LCPR' then 'Puerto Rico' when opco='CWP' then 'Panama' when opco='CWC' then 'Jamaica' else null end as Market
,MarketSize,Product,Biz_Unit,facet,journey_waypoint,kpi_name,kpi_meas as pNPS,kpi_num,kpi_den,KPI_Sla, kpi_delay_display,Network 
from "lla_cco_int_san"."cwp_ext_nps_kpis" --where opco='LCPR'
where kpi_name in('pNPS')
)
,rnps_kpi as(
select distinct date(date_parse(cast(month as varchar),'%Y%m%d')) as month,Opco,case when opco='CT' then 'Costa Rica' when opco='LCPR' then 'Puerto Rico' when opco='CWP' then 'Panama' when opco='CWC' then 'Jamaica' else null end as Market
,MarketSize,Product,Biz_Unit,facet,journey_waypoint,kpi_name,kpi_meas as rNPS,kpi_num,kpi_den,KPI_Sla, kpi_delay_display,Network 
from "lla_cco_int_san"."cwp_ext_nps_kpis" --where opco='LCPR'
where kpi_name in('rNPS')
)
select distinct f.month,f.market,f.network
,mtti,mttr,repairs,repairs_sl,installs_sl,rnps--,pnps
--,count(distinct churners) as churners
from service_delivery s 
full outer join rnps_kpi r on s.month=r.month and s.market=r.market and s.network=r.network
and s.month>=date('2022-01-01')
