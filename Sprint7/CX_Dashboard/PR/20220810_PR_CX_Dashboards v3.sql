with
--------------------------------Input Tables-------------------------------------------------------
fmc_table as(
select null as month,'LCPR' as Opco,'Puerto_Rico' as Market,'Large' as MarketSize,'Fixed' as Product,'B2C' as Biz_Unit,null as  facet,null as journey_waypoint,null as kpi_name,null as kpi_meas,null as kpi_num,null as kpi_den, null as Kpi_delay_display,null as Network
)
,service_delivery as(
select distinct Month,network,'LCPR' as Opco,'Puerto_Rico' as Market,'Large' as MarketSize,'Fixed' as Product,'B2C' as Biz_Unit,sum(Installations) as Install,round(sum(Inst_MTTI)/sum(Installations),2) as MTTI,sum(Repairs) as Repairs,round(sum(Rep_MTTR)/sum(Repairs),2) as MTTR,round(sum(scr),2) as Repairs_1k_rgu,round((sum(FTR_Install_M)/sum(Installations))/100,4) as FTR_Install,round((sum(FTR_Repair_M)/sum(Repairs))/100,4) as FTR_Repair
from( select distinct date_trunc('month',date(date_parse(cast(end_week_adj as varchar),'%Y%m%d'))) as Month,Network,date(date_parse(cast(end_week_adj as varchar),'%Y%m%d')) as End_Week,sum(Total_Subscribers) as Total_Users,sum(Assisted_Installations) as Installations,sum(mtti) as MTTI,sum(Assisted_Installations)*sum(mtti) as Inst_MTTI,sum(truck_rolls) as Repairs,sum(mttr) as MTTR,sum(truck_rolls)*sum(mttr) as Rep_MTTR,sum(scr) as SCR,(100-sum(i_elf_28days)) as FTR_Install,(100-sum(r_elf_28days)) as FTR_Repair,(100-sum(i_elf_28days))*sum(Assisted_Installations) as FTR_Install_M,(100-sum(r_elf_28days))*sum(truck_rolls) as FTR_Repair_M
from "lla_cco_int_san"."cwp_ext_servicedelivery_result" where market='Puerto Rico' --and network='OVERALL'
group by 1,2,3 order by 1,2,3) group by 1,2,3,4,5,6,7 order by 1,2,3,4,5,6,7
)
,nps_kpis as(
select distinct date(date_parse(cast(month as varchar),'%Y%m%d')) as month,Opco,Market,MarketSize,Product,Biz_Unit,facet,journey_waypoint,kpi_name,kpi_meas,kpi_num,kpi_den,KPI_Sla, kpi_delay_display,Network from "lla_cco_int_san"."cwp_ext_nps_kpis" where opco='LCPR')
,wanda_kpis as(
select date(date_parse(cast(month as varchar),'%Y%m%d')) as month,Opco,'Puerto_Rico' as market,'Large' as MarketSize,'Fixed' as Product,'B2C' as Biz_Unit,facet,journey_waypoint,kpi_name,kpi_meas,null as kpi_num,null as kpi_den,kpi_delay_display,network from "lla_cco_int_san"."cwp_ext_nps_wanda"  where opco='LCPR')
,digital_sales as(
select date(date_parse(cast(month as varchar),'%Y%m%d')) as month,opco,market,'Large' as MarketSize,'Fixed' as Product,'B2C' as Biz_Unit,kpi_name,kpi_meas,null as kpi_num,null as kpi_den,kpi_delay_display,kpi_sla,network
from "lla_cco_int_san"."cwp_ext_digitalsales" where opco='LCPR')
-------------------------------------Churn Dashboard kpis-----------------------------------------------------
,GrossAdds_Flag as(
select distinct month,Opco,Market,MarketSize,Product,Biz_Unit,'contact_drivers' as facet,'buy' as journey_waypoint,'Gross_Adds' as kpi_name,kpi_meas,kpi_num,kpi_den,Kpi_delay_display, Network from fmc_table)
,ActiveBase_Flag1 as(
select distinct month,Opco,Market,MarketSize,Product,Biz_Unit,'contact_drivers' as facet,'use' as journey_waypoint,'Active_Base' as kpi_name,kpi_meas,kpi_num,kpi_den,Kpi_delay_display,Network from fmc_table)
,ActiveBase_Flag2 as(
select distinct month,Opco,Market,MarketSize,Product,Biz_Unit,'contact_drivers' as facet,'support-call' as journey_waypoint,'Active_Base' as kpi_name,kpi_meas,kpi_num,kpi_den,Kpi_delay_display,Network  from fmc_table)
,TechTickets_Flag as (
select distinct month,Opco,Market,MarketSize,Product,Biz_Unit,'contact_intensity' as facet,'use' as journey_waypoint,'Tech_Tix_per_100_Acct' as kpi_name,kpi_meas,kpi_num,kpi_den,Kpi_delay_display,Network  from fmc_table)
,MRCChanges_Flag as(
select distinct month,Opco,Market,MarketSize,Product,Biz_Unit,'contact_drivers' as facet,'pay' as journey_waypoint,'Customers_w_MRC_Changes_5%+_Excl_Plan' as kpi_name,kpi_meas,kpi_num,kpi_den,Kpi_delay_display,Network  from fmc_table)
,SalesSoftDx_Flag as(
select distinct month,Opco,Market,MarketSize,Product,Biz_Unit,'high_risk' as facet,'buy' as journey_waypoint,'New_Sales_to_Soft_Dx' as kpi_name,kpi_meas,kpi_num,kpi_den,Kpi_delay_display,Network  from fmc_table)
,EarlyIssues_Flag as(
select distinct month,Opco,Market,MarketSize,Product,Biz_Unit,'high_risk' as facet,'buy' as journey_waypoint,'New_Customer_Callers_2+calls_21Days' as kpi_name,kpi_meas,kpi_num,kpi_den,Kpi_delay_display,Network  from fmc_table)
,LongInstall_Flag as(
select distinct month,Opco,Market,MarketSize,Product,Biz_Unit,'high_risk' as facet,'get' as journey_waypoint,'Breech_Cases_Install_6+Days' as kpi_name,kpi_meas,kpi_num,kpi_den,Kpi_delay_display,Network  from fmc_table)
,EarlyTickets_Flag as(
select distinct month,Opco,Market,MarketSize,Product,Biz_Unit,'high_risk' as facet,'get' as journey_waypoint,'Early_Tech_Tix_-7Weeks' as kpi_name,kpi_meas,kpi_num,kpi_den,Kpi_delay_display,Network  from fmc_table)
,RepeatedCall_Flag as (
select distinct month,Opco,Market,MarketSize,Product,Biz_Unit,'high_risk' as facet,'support-call' as journey_waypoint,'Repeat_Callers_2+Calls' as kpi_name,kpi_meas,kpi_num,kpi_den,Kpi_delay_display,Network  from fmc_table)
,OutlierRepair_Flag as (
select distinct month,Opco,Market,MarketSize,Product,Biz_Unit,'high_risk' as facet,'support-tech' as journey_waypoint,'Breech_Cases_Repair_4+Days' as kpi_name,kpi_meas,kpi_num,kpi_den,Kpi_delay_display,Network  from fmc_table)
,MountingBill_Flag as(
select distinct month,Opco,Market,MarketSize,Product,Biz_Unit,'high_risk' as facet,'pay' as journey_waypoint,'Customers_w_Mounting_Bills' as kpi_name,kpi_meas,kpi_num,kpi_den,Kpi_delay_display,Network  from fmc_table)
-------------------------------------Service Delivery Kpis---------------------------------------------------
,installs as(
select distinct month,Opco,Market,MarketSize,Product,Biz_Unit,'contact_drivers' as facet,'get' as journey_waypoint,'Installs' as kpi_name, Install as kpi_meas, null as kpi_num,	null as kpi_den, 'M-0' as Kpi_delay_display,Network from service_delivery)
,MTTI as(
select distinct month,Opco,Market,MarketSize,Product,Biz_Unit,'customer_time' as facet,'get' as journey_waypoint,'MTTI' as kpi_name, mtti as kpi_meas, null as kpi_num,null as kpi_den, 'M-0' as Kpi_delay_display, Network from service_delivery)
,ftr_installs as(
select distinct month,Opco,Market,MarketSize,Product,Biz_Unit,'effectiveness' as facet,'get' as journey_waypoint,'FTR_Installs' as kpi_name, ftr_install as kpi_meas, null as kpi_num,	null as kpi_den, 'M-0' as Kpi_delay_display, Network from service_delivery)
,justrepairs as(
select distinct month,Opco,Market,MarketSize,Product,Biz_Unit,'contact_drivers' as facet,'support-tech' as journey_waypoint,'Repairs' as kpi_name, repairs as kpi_meas, null as kpi_num,	null as kpi_den, 'M-0' as Kpi_delay_displa,Network from service_delivery)
,mttr as(
select distinct month,Opco,Market,MarketSize,Product,Biz_Unit,'customer_time' as facet,'support-tech' as journey_waypoint,'MTTR' as kpi_name, mttr as kpi_meas, null as kpi_num,	null as kpi_den, 'M-0' as Kpi_delay_display, Network from service_delivery)
,ftrrepair as(
select distinct month,Opco,Market,MarketSize,Product,Biz_Unit,'effectiveness' as facet,'support-tech' as journey_waypoint,'FTR_Repair' as kpi_name, ftr_repair as kpi_meas, null as kpi_num,	null as kpi_den, 'M-0' as Kpi_delay_display,Network from service_delivery)
,repairs1k as(
select distinct month,Opco,Market,MarketSize,Product,Biz_Unit,'contact_intensity' as facet,'support-tech' as journey_waypoint,'Repairs_per_1k_RGU' as kpi_name, Repairs_1k_rgu as kpi_meas, null as kpi_num,	null as kpi_den, 'M-0' as Kpi_delay_display,Network from service_delivery)
-------------------------------------NPS Kpis-----------------------------------------------------
,tBuy as(
select distinct month,Opco,Market,MarketSize,Product,Biz_Unit,facet,journey_waypoint,kpi_name,round(kpi_meas,2) as kpi_meas,null as kpi_num,null as kpi_den, Kpi_delay_display,Network from nps_kpis where kpi_name='tBuy')
,tinstall as(
select distinct month,Opco,Market,MarketSize,Product,Biz_Unit,facet,journey_waypoint,kpi_name,round(kpi_meas,2) as kpi_meas,null as kpi_num,null as kpi_den, Kpi_delay_display,Network from nps_kpis where kpi_name='tInstall')
,tpay as(
select distinct month,Opco,Market,MarketSize,Product,Biz_Unit,facet,journey_waypoint,'tPay' as kpi_name, round(kpi_meas,2) as kpi_meas, null as kpi_num,	null as kpi_den, Kpi_delay_display,Network from nps_kpis where kpi_name='tpay')
,helpcare as(
select distinct month,Opco,Market,MarketSize,Product,Biz_Unit,facet,journey_waypoint,kpi_name, round(kpi_meas,2) as kpi_meas, null as kpi_num,null as kpi_den, Kpi_delay_display,Network from nps_kpis where kpi_name='tHelp_Care')
,helprepair as(
select distinct month,Opco,Market,MarketSize,Product,Biz_Unit,facet,journey_waypoint,'tHelp_Repair' as kpi_name, round(kpi_meas,2) as kpi_meas, null as kpi_num,null as kpi_den, Kpi_delay_display,Network from nps_kpis where kpi_name='tHelp_repair')
,pnps as(
select distinct month,Opco,Market,MarketSize,Product,Biz_Unit,facet,journey_waypoint,kpi_name, round(kpi_meas,2) as kpi_meas, null as kpi_num,null as kpi_den, Kpi_delay_display,Network from nps_kpis where kpi_name='pNPS')
,rnps as(
select distinct month,Opco,Market,MarketSize,Product,Biz_Unit,facet,journey_waypoint,kpi_name,kpi_meas, null as kpi_num,null as kpi_den,Kpi_delay_display,Network from nps_kpis where kpi_name='rNPS')
-------------------------------------Wanda Kpis-----------------------------------------------------
,cccare as(
select distinct month,Opco,Market,MarketSize,Product,Biz_Unit,'customer_time' as facet,'support-call' as journey_waypoint,kpi_name,kpi_meas,kpi_num,kpi_den,Kpi_delay_display,Network from wanda_kpis where kpi_name='CC_SL_Care')
,cctech as(
select distinct month,Opco,Market,MarketSize,Product,Biz_Unit,'customer_time' as facet,'support-call' as journey_waypoint,kpi_name,kpi_meas,kpi_num,kpi_den,Kpi_delay_display,Network from wanda_kpis where kpi_name='CC_SL_Tech')
,chatbot as(
select distinct month,Opco,Market,MarketSize,Product,Biz_Unit,'digital_shift' as facet,'support-call' as journey_waypoint,kpi_name,kpi_meas,kpi_num,kpi_den,Kpi_delay_display,Network from wanda_kpis where kpi_name='Chatbot_Containment_Care')
,carecall as(
select distinct  month,Opco,Market,MarketSize,Product,Biz_Unit,'contact_intensity' as facet,'support-call' as journey_waypoint,kpi_name,kpi_meas,kpi_num,kpi_den,Kpi_delay_display,Network from wanda_kpis where kpi_name='Care_Calls_Intensity')
,techcall as(
select distinct  month,Opco,Market,MarketSize,Product,Biz_Unit,'contact_intensity' as facet,'support-call' as journey_waypoint,kpi_name,kpi_meas,kpi_num,kpi_den,Kpi_delay_display,Network from wanda_kpis where kpi_name='Tech_Calls_Intensity')
,chahtbottech as(
select distinct  month,Opco,Market,MarketSize,Product,Biz_Unit,'digital_shift' as facet,'support-tech' as journey_waypoint,kpi_name,kpi_meas,kpi_num,kpi_den,Kpi_delay_display,Network from wanda_kpis where kpi_name='Chatbot_Containment_Tech')
,frccare as(
select distinct month,Opco,Market,MarketSize,Product,Biz_Unit,'effectiveness' as facet,'support-call' as journey_waypoint,'FCR_Care' as kpi_name,kpi_meas,kpi_num,kpi_den,Kpi_delay_display,Network from wanda_kpis where kpi_name='FRC_Care')
-------------------------------------Other Kpis-----------------------------------------------------
,highrisk as(
select distinct month,Opco,Market,MarketSize,Product,Biz_Unit,'high_risk' as facet,'use' as journey_waypoint,'High_Tech_Call_Nodes_+6%Monthly' as kpi_name, kpi_meas, kpi_num,kpi_den, Kpi_delay_display, Network from fmc_table)
,payments as(
select distinct month,opco,market,marketsize,product,biz_unit,'digital_shift' as facet,'pay' as journey_waypoint,'Digital_Payments' as kpi_name, kpi_meas, kpi_num,kpi_den, Kpi_delay_display, Network from fmc_table)
,ecommerce as(
select distinct month,Opco,Market,MarketSize,Product,Biz_Unit,'digital_shift' as facet,'buy' as journey_waypoint,'e-Commerce' as kpi_name,kpi_meas,kpi_num,kpi_den,Kpi_delay_display,Network,kpi_sla from digital_sales)
,ftr_billing as(
select distinct month,Opco,Market,MarketSize,Product,Biz_Unit,'effectiveness' as facet,'pay' as journey_waypoint,'FTR_Billing' as kpi_name, kpi_meas, kpi_num,kpi_den, Kpi_delay_display, Network from fmc_table)
,installscalls as(
select distinct month,Opco,Market,MarketSize,Product,Biz_Unit,'contact_intensity' as facet,'get' as journey_waypoint,'Install_Calls/Installs' as kpi_name, kpi_meas, kpi_num,kpi_den, Kpi_delay_display, Network from fmc_table)
,MTTBTR as(
select distinct month,Opco,Market,MarketSize,Product,Biz_Unit,'customer_time' as facet,'pay' as journey_waypoint,'MTTBTR' as kpi_name, kpi_meas, kpi_num,kpi_den, Kpi_delay_display, Network from fmc_table)
,selfinstalls as(
select distinct month,Opco,Market,MarketSize,Product,Biz_Unit,'digital_shift' as facet,'get' as journey_waypoint,'Self_Installs' as kpi_name, kpi_meas, kpi_num,kpi_den, Kpi_delay_display, Network from fmc_table)
,mttb as(
select distinct month,Opco,Market,MarketSize,Product,Biz_Unit,'customer_time' as facet,'buy' as journey_waypoint,'MTTB' as kpi_name, kpi_meas, kpi_num,kpi_den, Kpi_delay_display, Network from fmc_table)
,Buyingcalls as(
select distinct month,Opco,Market,MarketSize,Product,Biz_Unit,'contact_intensity' as facet,'buy' as journey_waypoint,'Buying_Calls/GA' as kpi_name, kpi_meas, kpi_num,kpi_den, Kpi_delay_display, Network from fmc_table)
,billbill as(
select distinct  month,Opco,Market,MarketSize,Product,Biz_Unit,'contact_intensity' as facet,'pay' as journey_waypoint,'Billing Calls per Bill Variation' as kpi_name,kpi_meas,kpi_num,kpi_den,Kpi_delay_display,Network from fmc_table)
-----------------------------------------join--------------------------------------
,join_churn as (
select * from GrossAdds_Flag union all select * from ActiveBase_Flag1 union all select * from ActiveBase_Flag2 union all select * from TechTickets_Flag union all select * from MRCChanges_Flag union all select * from SalesSoftDx_Flag union all select * from EarlyIssues_Flag union all select * from LongInstall_Flag union all select * from EarlyTickets_Flag union all select * from RepeatedCall_Flag union all select * from MountingBill_Flag)
,join_service_delivery as(
select * from join_churn union all select * from installs union all select * from MTTI union all select * from ftr_installs union all select * from justrepairs union all select * from mttr union all select * from ftrrepair union all select * from repairs1k)
,join_nps as(
select * from join_service_delivery union all select * from tBuy union all select * from tinstall union all select * from tpay union all select * from helpcare union all select * from helprepair union all select * from pnps union all select * from rnps)
,join_wanda as(
select * from join_nps union all select * from billbill union all select * from cccare union all select * from cctech union all select * from chatbot union all select * from carecall union all select * from techcall union all select * from chahtbottech)
,join_others as(
select *,null as kpi_sla from(select * from join_wanda union all select * from highrisk union all select * from payments union all select * from frccare ftr_billing union all select * from installscalls union all select * from MTTBTR union all select * from selfinstalls union all select * from  mttb union all select * from Buyingcalls)
union all select * from ecommerce
)
select distinct month,Opco,Market,MarketSize,Product,Biz_Unit,facet,journey_waypoint,kpi_name,kpi_meas,kpi_num,kpi_den, kpi_sla,Kpi_delay_display,null as kpi_disclaimer_display,null as kpi_disclaimer_meas,Network,year(Month) as ref_year,month(month) as ref_mo,null as kpi_sla_below_threshold,null as kpi_sla_middling_threshold,null as kpi_sla_above_threshold,null as kpi_sla_far_below_threshold,null as kpi_sla_far_above_threshold
--facet,journey_waypoint,kpi_name
from join_others
--where month=date('2022-05-01')
--order by 1,2,3
