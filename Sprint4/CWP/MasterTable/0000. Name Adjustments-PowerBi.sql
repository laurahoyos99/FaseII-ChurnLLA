with 
powerbi as(
SELECT 
month as Month,
finalaccount as FinalAccount,
final_bom_activeflag as Final_BOM_ActiveFlag,
final_eom_activeflag as Final_EOM_ActiveFlag,
fmcflag	 as FmcFlag,
fixedaccount as FixedAccount,
f_activebom as F_ActiveBOM,
f_activeeom	as F_ActiveEOM,
fix_b_date as Fix_B_Date,
fixed_b_phone as Fixed_B_Phone,
b_overdue as B_Overdue,
fixed_b_maxstart as Fixed_B_MaxStart,
b_fixedtenure as B_FixedTenure,
cast(round(b_fixed_mrc,0) as int) as B_Fixed_MRC,
b_techflag as B_TechFlag,
b_numrgus as B_NumRGUs,
b_mixname_adj as B_MixName_Adj,
b_mixcode_adj as B_MixCode_Adj,
b_bbcode as B_bbCode,
b_tvcode as B_tvCode,
b_vocode as B_voCode,
b_hard_fmc_flag as B_Hard_FMC_Flag,
fix_e_date as Fix_E_Date,
fixed_e_phone as Fixed_E_Phone,
e_overdue as E_Overdue,
fixed_e_maxstart as Fixed_E_MaxStart,
e_fixedtenure as E_FixedTenure,
cast(round(e_fixed_mrc,0) as int)	 as E_Fixed_MRC,
e_techflag	 as E_TechFlag,
e_numrgus as E_NumRGUs,
e_mixname_adj as E_MixName_Adj,
e_mixcode_adj as E_MixCode_Adj	,
e_bbcode as E_bbCode, 
e_tvcode as E_tvCode,
e_vocode as E_voCode,
e_hard_fmc_flag as E_Hard_FMC_Flag,
first_sales_chnl_bom,last_sales_chnl_bom,	first_sales_chnl_eom,	last_sales_chnl_eom,
fixedmainmovement as FixedMainMovement,
fixedspinmovement as FixedSpinMovement,
fixedchurnflag as FixedChurnFlag,
fixedchurntype as FixedChurnType,
--FixedChurnSubtype AS FixedChurnSubtype,
fmcflagfix as FMCFlagFix,
mobile_account as Mobile_Account,
phonenumber as PhoneNumber,
mobile_activebom as Mobile_ActiveBOM,
mobile_activeeom as Mobile_ActiveEOM,
b_date as B_Date,
phone_bom, Phone_BOM,
mobile_b_maxstart as Mobile_B_MaxStart,
b_mob_acc_name as B_Mob_Acc_Name,
b_mobile_id as B_Mobile_ID,
cast(round(b_mobilemrc,0) as int) as B_MobileMRC,
b_mobilergus as B_MobileRGUs,
cast(round(b_avgmobilemrc,0) as int) as B_AvgMobileMRC,
b_mobiletenure as B_MobileTenure,
e_date as E_Date,
phone_eom as Phone_EOM,
mobile_e_maxstart as Mobile_E_MaxStart,
e_mob_acc_name as E_Mob_Acc_Name,
e_mobile_id as E_Mobile_ID,
cast( round(e_mobilemrc,0) as int) as E_MobileMRC,
e_mobilergus as E_MobileRGUs,
cast( round(e_avgmobilemrc,0) as int)	as E_AvgMobileMRC,
e_mobiletenure as E_MobileTenure,
cast(round(mobile_mrc_diff,0) as int) as Mobile_MRC_Diff,
mobilemainmovement as MobileMainMovement,
mobilespinflag as MobileSpinFlag,
fmcflagmob as FmcFlagMob,
drc	as DRC,
mobilechurnflag as MobileChurnFlag,
mobilechurnertype as MobileChurnerType,
finalchurnflag as FinalChurnFlag,
churntypefinalflag as ChurnTypeFinalFlag,
b_final_tenure as B_Final_Tenure,
e_final_tenure as E_Final_Tenure,
b_final_techflag as B_Final_TechFlag,
e_final_techflag as E_Final_TechFlag,
b_fmctype as B_FMCType,
e_fmctype as E_FMCType,
b_fmcsegment as B_FMCSegment,
e_fmcsegment as E_FMCSegment,
cast(round(b_total_mrc,0) as int) as B_Total_MRC,
cast(round(e_total_mrc,0) as int) as E_Total_MRC,
rejoinerflag as RejoinerFlag,
rejoinerfmcflag as RejoinerFMCFlag,
waterfall_flag as Waterfall_Flag
FROM "lla_cco_int_stg"."cwp_sp3_basekpis_dashboardinput_dinamico_RJ" where month = date('2022-02-01') 
order by finalaccount
)
select distinct * -- month,count(distinct finalaccount)
from powerbi
--where final_eom_activeflag=1 and (drc=1 or drc is null)
--where waterfall_flag is null
--group by month