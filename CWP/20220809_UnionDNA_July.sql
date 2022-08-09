CREATE TABLE IF NOT EXISTS "lla_cco_int_stg"."cwp_fix_union_dna" as

WITH

UsefulDays_DEV as(
SELECT act_acct_cd,act_acct_name,act_blng_cycl,act_cust_typ,act_cust_typ_nm,act_cust_type_grp,act_mktg_cat_cd,act_mktg_cat_nm,act_crdt_cntrl_typ_cd,act_crdt_cntrl_typ_nm,act_acct_stat,act_cust_strt_dt,act_contact_phone_1,act_contact_phone_2,act_contact_phone_3,act_contact_mail_1,act_contact_mail_2,act_contact_mail_3,act_acct_inst_dt,act_acct_mgr_cd,act_acct_mgr_nm,act_rgn_cd,act_area_cd,act_prvnc_cd,act_credit_score,act_segment_prpty_nm,act_soc_ecnmc_scr,act_bill_street,act_bill_neighborhood,act_bill_city,act_bill_county,act_bill_province,act_bill_project,act_bill_vdp,act_blng_addr,act_acct_id_typ,act_acct_id_val,act_acct_typ_grp,act_mgr_nm,act_chrn_flg,act_chrn_typ,cst_cust_cd,cst_cust_name,cst_alias,c_cust_age,c_acct_age,cst_gndr,cst_empmnt,act_soc_ecnmc_scr_m1,act_credit_score_m1,act_segment_prpty_nm_m1,act_soc_ecnmc_scr_m2,act_credit_score_m2,act_segment_prpty_nm_m2,act_soc_ecnmc_scr_m3,act_credit_score_m3,act_segment_prpty_nm_m3,cst_mrtl_stat,cst_edu_lvl,cst_income,cst_currency,cst_prfrd_cntct_mthd_nm,cst_chrn_rsn,cst_chrn_dt,evt_frst_sale_rep_cd,evt_frst_sale_rep_nm,evt_frst_sale_chnl,evt_lst_sale_rep_cd,evt_lst_sale_rep_nm,evt_lst_sale_chnl,evt_usg_dur_m0_loc,evt_usg_dur_m1_loc,evt_usg_dur_m2_loc,evt_usg_dur_m3_loc,evt_usg_amt_m0_loc,evt_usg_amt_m1_loc,evt_usg_amt_m2_loc,evt_usg_amt_m3_loc,evt_usg_dur_m0_ldn,evt_usg_dur_m1_ldn,evt_usg_dur_m2_ldn,evt_usg_dur_m3_ldn,evt_usg_amt_m0_ldn,evt_usg_amt_m1_ldn,evt_usg_amt_m2_ldn,evt_usg_amt_m3_ldn,evt_usg_dur_m0_ldi,evt_usg_dur_m1_ldi,evt_usg_dur_m2_ldi,evt_usg_dur_m3_ldi,evt_usg_amt_m0_ldi,evt_usg_amt_m1_ldi,evt_usg_amt_m2_ldi,evt_usg_amt_m3_ldi,evt_usg_tv_events_m0,evt_usg_tv_events_m1,evt_usg_tv_events_m2,evt_usg_tv_events_m3,evt_usg_tv_dur_m0,evt_usg_tv_dur_m1,evt_usg_tv_dur_m2,evt_usg_tv_dur_m3,evt_usg_replay_events_m0,evt_usg_replay_events_m1,evt_usg_replay_events_m2,evt_usg_replay_events_m3,evt_usg_pvr_events_m0,evt_usg_pvr_events_m1,evt_usg_pvr_events_m2,evt_usg_pvr_events_m3,evt_usg_err_events_m0,evt_usg_err_events_m1,evt_usg_err_events_m2,evt_usg_err_events_m3,evt_usg_glitch_events_m0,evt_usg_glitch_events_m1,evt_usg_glitch_events_m2,evt_usg_glitch_events_m3,evt_usr_vod_events_m0,evt_usr_vod_events_m1,evt_usr_vod_events_m2,evt_usr_vod_events_m3,evt_truckrolls_events_m0,evt_truckrolls_events_m1,evt_truckrolls_events_m2,evt_truckrolls_events_m3,evt_fault_tickets_events_m0,evt_fault_tickets_events_m1,evt_fault_tickets_events_m2,evt_fault_tickets_events_m3,evt_massive_tickets_events_m0,evt_massive_tickets_events_m1,evt_massive_tickets_events_m2,evt_massive_tickets_events_m3,evt_cc_tickets_events_m0,evt_cc_tickets_events_m1,evt_cc_tickets_events_m2,evt_cc_tickets_events_m3,evt_cc_cl_out_curr_qty,evt_cc_cl_in_curr_qty,evt_mail_snd_curr_qty,evt_mail_rcv_curr_qty,evt_sms_snd_curr_qty,evt_pst_mail_curr_qty,evt_app_evt_curr_qty,evt_str_vst_curr_qty,evt_usg_dur_m1_mob,evt_usg_dur_m2_mob,evt_usg_dur_m3_mob,evt_usg_dur_m1_loc_off,evt_usg_dur_m1_loc_onn,evt_usg_dur_m2_loc_off,evt_usg_dur_m2_loc_onn,evt_usg_dur_m3_loc_off,evt_usg_dur_m3_loc_onn,evt_usg_dur_m1_ldn_off,evt_usg_dur_m1_ldn_onn,evt_usg_dur_m2_ldn_off,evt_usg_dur_m2_ldn_onn,evt_usg_dur_m3_ldn_off,evt_usg_dur_m3_ldn_onn,evt_usg_amt_m1_mob,evt_usg_amt_m2_mob,evt_usg_amt_m3_mob,evt_cc_calls_out_m1_qty,evt_cc_calls_out_m2_qty,evt_cc_calls_out_m3_qty,evt_cc_calls_in_m1_qty,evt_cc_calls_in_m2_qty,evt_cc_calls_in_m3_qty,evt_emails_sended_qty_m1,evt_emails_sended_qty_m2,evt_emails_sended_qty_m3,evt_emails_received_qty_m1,evt_emails_received_qty_m2,evt_emails_received_qty_m3,evt_sms_sent_m1_qty,evt_sms_sent_m2_qty,evt_sms_sent_m3_qty,evt_post_mail_m1_qty,evt_post_mail_m2_qty,evt_post_mail_m3_qty,evt_app_events_m1_qty,evt_app_events_m2_qty,evt_app_events_m3_qty,evt_store_visits_m1_qty,evt_store_visits_m2_qty,evt_store_visits_m3_qty,fi_tot_mrc_qty,fi_tot_mrc_amt,fi_vo_mrc_qty,fi_vo_mrc_amt,fi_bb_mrc_qty,fi_bb_mrc_amt,fi_tv_mrc_qty,fi_tv_mrc_amt,fi_tot_dscnt_qty,fi_tot_dscnt_amt,fi_vo_dscnt_qty,fi_vo_dscnt_amt,fi_bb_dscnt_qty,fi_bb_dscnt_amt,fi_tv_dscnt_qty,fi_tv_dscnt_amt,fi_vo_rntl_prcnt,fi_bb_rntl_prcnt,fi_tv_rntl_prcnt,fi_tot_srv_chrg_qty,fi_tot_srv_chrg_amt,fi_vo_srv_chrg_qty,fi_bb_srv_chrg_qty,fi_tv_srv_chrg_qty,fi_vo_srv_chrg_amt,fi_bb_srv_chrg_amt,fi_tv_srv_chrg_amt,fi_tot_srv_chrg_qty_pos,fi_tot_srv_chrg_amt_pos,fi_vo_srv_chrg_qty_pos,fi_bb_srv_chrg_qty_pos,fi_tv_srv_chrg_qty_pos,fi_vo_srv_chrg_amt_pos,fi_bb_srv_chrg_amt_pos,fi_tv_srv_chrg_amt_pos,fi_tot_srv_chrg_qty_neg,fi_tot_srv_chrg_amt_neg,fi_vo_srv_chrg_qty_neg,fi_bb_srv_chrg_qty_neg,fi_tv_srv_chrg_qty_neg,fi_vo_srv_chrg_amt_neg,fi_bb_srv_chrg_amt_neg,fi_tv_srv_chrg_amt_neg,fi_tot_inst_chrg,fi_vo_inst_chrg,fi_bb_inst_chrg,fi_tv_inst_chrg,fi_bill_amt_m0,fi_bill_amt_m1,fi_bill_amt_m2,fi_bill_amt_m3,fi_pmnt_mthd_nm_m0,fi_pmnt_mthd_nm_m1,fi_pmnt_mthd_nm_m2,fi_pmnt_mthd_nm_m3,fi_pmnt_amt_m0,fi_pmnt_amt_m1,fi_pmnt_amt_m2,fi_pmnt_amt_m3,fi_outst_age,fi_outst_amt,fi_outst_amt_curr,fi_outst_amt_30,fi_outst_amt_60,fi_outst_amt_90,fi_outst_amt_more_90,fi_outst_amt_write_off,fi_tot_mrc_amt_prev,fi_vo_mrc_amt_prev,fi_bb_mrc_amt_prev,fi_tv_mrc_amt_prev,fi_tot_mrc_qty_prev,fi_vo_mrc_qty_prev,fi_bb_mrc_qty_prev,fi_tv_mrc_qty_prev,fi_tot_dscnt_amt_prev,fi_tot_dscnt_qty_prev,fi_vo_dscnt_amt_prev,fi_vo_dscnt_qty_prev,fi_bb_dscnt_amt_prev,fi_bb_dscnt_qty_prev,fi_tv_dscnt_amt_prev,fi_tv_dscnt_qty_prev,fi_vo_rntl_prcnt_prev,fi_bb_rntl_prcnt_prev,fi_tv_rntl_prcnt_prev,fi_tot_srv_chrg_amt_prev,fi_vo_srv_chrg_amt_prev,fi_bb_srv_chrg_amt_prev,fi_tv_srv_chrg_amt_prev,fi_tot_srv_chrg_qty_prev,fi_vo_srv_chrg_qty_prev,fi_bb_srv_chrg_qty_prev,fi_tv_srv_chrg_qty_prev,nr_short_node,nr_long_node,nr_terminal,nr_minibox,nr_cable,nr_tel_center,nr_odfx,nr_fdh,nr_fdp,nr_ont,nr_tv_stb_free_qty,nr_tv_stb_qty,nr_max_down,nr_bb_mac,nr_prjct_dt,nr_prjct_typ,nr_suit_fr_dsl,pd_mix_cd,pd_mix_nm,pd_vo_prod_cd,pd_vo_prod_nm,pd_vo_fmly,pd_vo_sbfmly,pd_vo_accs_media,pd_vo_tech,pd_vo_prd_inst_date,pd_bb_prod_cd,pd_bb_prod_nm,pd_bb_fmly,pd_bb_sbfmly,pd_bb_accs_media,pd_bb_tech,pd_bb_speed,pd_bb_prd_inst_date,pd_tv_prod_cd,pd_tv_prod_nm,pd_tv_fmly,pd_tv_sbfmly,pd_tv_accs_media,pd_tv_tech,pd_tv_prd_inst_date,pd_vo_prod_cd_prev,pd_vo_prod_nm_prev,pd_vo_accs_media_prev,pd_vo_tech_prev,pd_bb_prod_cd_prev,pd_bb_prod_nm_prev,pd_bb_accs_media_prev,pd_bb_tech_prev,pd_tv_prod_cd_prev,pd_tv_prod_nm_prev,pd_tv_accs_media_prev,pd_tv_tech_prev,pd_vo_prd_inst_date_prev,pd_bb_prd_inst_date_prev,pd_tv_prd_inst_date_prev,pd_vo_fmly_prev,pd_vo_sbfmly_prev,pd_bb_fmly_prev,pd_bb_sbfmly_prev,pd_tv_fmly_prev,pd_tv_sbfmly_prev,pd_bb_speed_prev,pd_mix_cd_prev,pd_mix_nm_prev,srv_phy_street,srv_phy_neighborhood,srv_phy_city,srv_phy_county,srv_phy_province,srv_phy_project,srv_phy_vdp,srv_phy_addr,srv_curr_vo_qty_accs_mthd,srv_curr_bb_qty_accs_mthd,srv_curr_tv_qty_accs_mthd,srv_vo_qty_accs_mthd_m1,srv_bb_qty_accs_mthd_m1,srv_tv_qty_accs_mthd_m1,srv_vo_qty_accs_mthd_m2,srv_bb_qty_accs_mthd_m2,srv_tv_qty_accs_mthd_m2,srv_vo_qty_accs_mthd_m3,srv_bb_qty_accs_mthd_m3,srv_tv_qty_accs_mthd_m3,org_cntry,load_dt,mo_key,day_key,fi_bill_dt_m0,fi_bill_dt_m1,fi_bill_dt_m2,fi_bill_dt_m3,fi_bill_due_dt_m0,fi_bill_due_dt_m1,fi_bill_due_dt_m2,fi_bill_due_dt_m3,fi_bill_pmnt_dt_m0,fi_bill_pmnt_dt_m1,fi_bill_pmnt_dt_m2,fi_bill_pmnt_dt_m3,fi_bill_clr_dt_m0,fi_bill_clr_dt_m1,fi_bill_clr_dt_m2,fi_bill_clr_dt_m3,act_flowid_mail,act_flowid_key,act_flowid_dt,cntry_id,fmc_flag,fmc_org_cntry,fmc_household_id,fmc_start_date,fmc_end_date,fmc_status,fmc_status_change_date,bundle_code,bundle_name,bundle_inst_date,bundle_cease_date,fmc_sales_channel_type,fmc_sales_rep_id,service_id,bundle_code_prev,bundle_name_prev,bundle_inst_date_prev,bundle_cease_date_prev,fmc_reporting_action,fmc_vo_prod_cd_prev,fmc_vo_prod_nm_prev,fmc_vo_prod_inst_date,fmc_tv_prod_cd_prev,fmc_tv_prod_nm_prev,fmc_tv_prod_inst_date_prev,fmc_bb_prod_cd_prev,fmc_bb_prod_nm_prev,fmc_bb_prd_inst_date_prev,fmc_vo_tech_prev,fmc_bb_tech_prev,fmc_tv_tech_prev,fmc_bb_speed_prev,fmc_vo_tech,fmc_bb_tech,fmc_tv_tech,fi_vo_mrc_amt_prev_fmc,fi_bb_mrc_amt_prev_fmc,fi_tv_mrc_amt_prev_fmc,fmc_load_date,fmc_dt_yr_mth,pd_min_inst_dt_curr,pd_min_inst_dt_yr_mth,pd_mix_cd_prev_fmc,pd_mix_nm_prev_fmc,pd_min_inst_dt_prev,fi_tot_mrc_amt_prev_fmc,fmc_reporting_action_level_1,fmc_reporting_action_level_2,mo_tot_mrc_amt_prev,mo_tot_mrc_amt,fi_outst_amt_4_mths,fi_outst_amt_5_mths,fi_outst_amt_6_mths,fi_outst_amt_7_mths,fi_outst_amt_8_mths,fi_outst_amt_9_mths,fi_outst_amt_10_mths,fi_outst_amt_11_mths,fi_outst_amt_12_mths,fi_outst_amt_12_mths_plus,fi_overdue_age,fi_overdue_amt_1_mth,fi_overdue_amt_2_mths,fi_overdue_amt_3_mths,fi_overdue_amt_4_mths,fi_overdue_amt_5_mths,fi_overdue_amt_6_mths,fi_overdue_amt_7_mths,fi_overdue_amt_8_mths,fi_overdue_amt_9_mths,fi_overdue_amt_10_mths,fi_overdue_amt_11_mths,fi_overdue_amt_12_mths,fi_overdue_amt_12_mths_plus,fi_tot_srv_chrg_cnt,fi_tot_srv_chrg_cnt_neg,fi_tot_srv_chrg_cnt_pos,fmc_sales_channel_id,acct_stat_prev,bar_cnt_6mths,bb_cpe,bill_day,bills_cnt_6mths,bounced_paym_cnt_6mths,days_at_acct_stat_curr,days_at_acct_stat_prev,default_bill_cnt_6mths,default_cnt_6mths,direct_debit_flag,disputes_cnt_6mths,no_overdue_bills,churn_score_dt,churninv_score,churininv_centil,lgrossmarg,lgrossmarg_centil,oldest_unpaid_bill_dt,oldest_unpaid_due_dt,paym_dt_vs_due_dt_3ma,paym_plan_flag,promise_to_pay_cnt_6mths,srv_curr_mo_qty_accs_mthd,srv_mo_qty_accs_mthd_m1,tos_cnt_6mths,total_charges_curr,total_charges_prev,total_credit_amt,total_deposit_amt,total_dispute_amt,total_nrc_amt,total_nrc_amt_prev,total_overdue_amt,tv_cpe,null as act_self_install_flg,dt
  FROM "db-analytics-dev"."dna_fixed_cwp" 
--WHERE date(dt) not in (date('2021-11-25'), date('2021-11-26'), date('2021-11-27'), date('2021-11-28'), date('2021-11-29'), date('2021-11-30'), date('2021-12-01'), date('2021-12-02'), date('2021-12-03'), date('2021-12-04'),date('2021-12-05'), date('2021-02-03'),date('2021-02-04'), date('2021-04-02'))
--AND act_cust_typ_nm = 'Residencial'
--AND act_acct_typ_grp ='MAS MOVIL'
)
,
UsefulDays_PROD as (
SELECT act_acct_cd,act_acct_name,act_blng_cycl,act_cust_typ,act_cust_typ_nm,act_cust_type_grp,act_mktg_cat_cd,act_mktg_cat_nm,act_crdt_cntrl_typ_cd,act_crdt_cntrl_typ_nm,act_acct_stat,act_cust_strt_dt,act_contact_phone_1,act_contact_phone_2,act_contact_phone_3,act_contact_mail_1,act_contact_mail_2,act_contact_mail_3,act_acct_inst_dt,act_acct_mgr_cd,act_acct_mgr_nm,act_rgn_cd,act_area_cd,act_prvnc_cd,act_credit_score,act_segment_prpty_nm,act_soc_ecnmc_scr,act_bill_street,act_bill_neighborhood,act_bill_city,act_bill_county,act_bill_province,act_bill_project,act_bill_vdp,act_blng_addr,act_acct_id_typ,act_acct_id_val,act_acct_typ_grp,act_mgr_nm,act_chrn_flg,act_chrn_typ,cst_cust_cd,cst_cust_name,cst_alias,c_cust_age,c_acct_age,cst_gndr,cst_empmnt,act_soc_ecnmc_scr_m1,act_credit_score_m1,act_segment_prpty_nm_m1,act_soc_ecnmc_scr_m2,act_credit_score_m2,act_segment_prpty_nm_m2,act_soc_ecnmc_scr_m3,act_credit_score_m3,act_segment_prpty_nm_m3,cst_mrtl_stat,cst_edu_lvl,cst_income,cst_currency,cst_prfrd_cntct_mthd_nm,cst_chrn_rsn,cst_chrn_dt,evt_frst_sale_rep_cd,evt_frst_sale_rep_nm,evt_frst_sale_chnl,evt_lst_sale_rep_cd,evt_lst_sale_rep_nm,evt_lst_sale_chnl,evt_usg_dur_m0_loc,evt_usg_dur_m1_loc,evt_usg_dur_m2_loc,evt_usg_dur_m3_loc,evt_usg_amt_m0_loc,evt_usg_amt_m1_loc,evt_usg_amt_m2_loc,evt_usg_amt_m3_loc,evt_usg_dur_m0_ldn,evt_usg_dur_m1_ldn,evt_usg_dur_m2_ldn,evt_usg_dur_m3_ldn,evt_usg_amt_m0_ldn,evt_usg_amt_m1_ldn,evt_usg_amt_m2_ldn,evt_usg_amt_m3_ldn,evt_usg_dur_m0_ldi,evt_usg_dur_m1_ldi,evt_usg_dur_m2_ldi,evt_usg_dur_m3_ldi,evt_usg_amt_m0_ldi,evt_usg_amt_m1_ldi,evt_usg_amt_m2_ldi,evt_usg_amt_m3_ldi,evt_usg_tv_events_m0,evt_usg_tv_events_m1,evt_usg_tv_events_m2,evt_usg_tv_events_m3,evt_usg_tv_dur_m0,evt_usg_tv_dur_m1,evt_usg_tv_dur_m2,evt_usg_tv_dur_m3,evt_usg_replay_events_m0,evt_usg_replay_events_m1,evt_usg_replay_events_m2,evt_usg_replay_events_m3,evt_usg_pvr_events_m0,evt_usg_pvr_events_m1,evt_usg_pvr_events_m2,evt_usg_pvr_events_m3,evt_usg_err_events_m0,evt_usg_err_events_m1,evt_usg_err_events_m2,evt_usg_err_events_m3,evt_usg_glitch_events_m0,evt_usg_glitch_events_m1,evt_usg_glitch_events_m2,evt_usg_glitch_events_m3,evt_usr_vod_events_m0,evt_usr_vod_events_m1,evt_usr_vod_events_m2,evt_usr_vod_events_m3,evt_truckrolls_events_m0,evt_truckrolls_events_m1,evt_truckrolls_events_m2,evt_truckrolls_events_m3,evt_fault_tickets_events_m0,evt_fault_tickets_events_m1,evt_fault_tickets_events_m2,evt_fault_tickets_events_m3,evt_massive_tickets_events_m0,evt_massive_tickets_events_m1,evt_massive_tickets_events_m2,evt_massive_tickets_events_m3,evt_cc_tickets_events_m0,evt_cc_tickets_events_m1,evt_cc_tickets_events_m2,evt_cc_tickets_events_m3,evt_cc_cl_out_curr_qty,evt_cc_cl_in_curr_qty,evt_mail_snd_curr_qty,evt_mail_rcv_curr_qty,evt_sms_snd_curr_qty,evt_pst_mail_curr_qty,evt_app_evt_curr_qty,evt_str_vst_curr_qty,evt_usg_dur_m1_mob,evt_usg_dur_m2_mob,evt_usg_dur_m3_mob,evt_usg_dur_m1_loc_off,evt_usg_dur_m1_loc_onn,evt_usg_dur_m2_loc_off,evt_usg_dur_m2_loc_onn,evt_usg_dur_m3_loc_off,evt_usg_dur_m3_loc_onn,evt_usg_dur_m1_ldn_off,evt_usg_dur_m1_ldn_onn,evt_usg_dur_m2_ldn_off,evt_usg_dur_m2_ldn_onn,evt_usg_dur_m3_ldn_off,evt_usg_dur_m3_ldn_onn,evt_usg_amt_m1_mob,evt_usg_amt_m2_mob,evt_usg_amt_m3_mob,evt_cc_calls_out_m1_qty,evt_cc_calls_out_m2_qty,evt_cc_calls_out_m3_qty,evt_cc_calls_in_m1_qty,evt_cc_calls_in_m2_qty,evt_cc_calls_in_m3_qty,evt_emails_sended_qty_m1,evt_emails_sended_qty_m2,evt_emails_sended_qty_m3,evt_emails_received_qty_m1,evt_emails_received_qty_m2,evt_emails_received_qty_m3,evt_sms_sent_m1_qty,evt_sms_sent_m2_qty,evt_sms_sent_m3_qty,evt_post_mail_m1_qty,evt_post_mail_m2_qty,evt_post_mail_m3_qty,evt_app_events_m1_qty,evt_app_events_m2_qty,evt_app_events_m3_qty,evt_store_visits_m1_qty,evt_store_visits_m2_qty,evt_store_visits_m3_qty,fi_tot_mrc_qty,fi_tot_mrc_amt,fi_vo_mrc_qty,fi_vo_mrc_amt,fi_bb_mrc_qty,fi_bb_mrc_amt,fi_tv_mrc_qty,fi_tv_mrc_amt,fi_tot_dscnt_qty,fi_tot_dscnt_amt,fi_vo_dscnt_qty,fi_vo_dscnt_amt,fi_bb_dscnt_qty,fi_bb_dscnt_amt,fi_tv_dscnt_qty,fi_tv_dscnt_amt,fi_vo_rntl_prcnt,fi_bb_rntl_prcnt,fi_tv_rntl_prcnt,fi_tot_srv_chrg_qty,fi_tot_srv_chrg_amt,fi_vo_srv_chrg_qty,fi_bb_srv_chrg_qty,fi_tv_srv_chrg_qty,fi_vo_srv_chrg_amt,fi_bb_srv_chrg_amt,fi_tv_srv_chrg_amt,fi_tot_srv_chrg_qty_pos,fi_tot_srv_chrg_amt_pos,fi_vo_srv_chrg_qty_pos,fi_bb_srv_chrg_qty_pos,fi_tv_srv_chrg_qty_pos,fi_vo_srv_chrg_amt_pos,fi_bb_srv_chrg_amt_pos,fi_tv_srv_chrg_amt_pos,fi_tot_srv_chrg_qty_neg,fi_tot_srv_chrg_amt_neg,fi_vo_srv_chrg_qty_neg,fi_bb_srv_chrg_qty_neg,fi_tv_srv_chrg_qty_neg,fi_vo_srv_chrg_amt_neg,fi_bb_srv_chrg_amt_neg,fi_tv_srv_chrg_amt_neg,fi_tot_inst_chrg,fi_vo_inst_chrg,fi_bb_inst_chrg,fi_tv_inst_chrg,fi_bill_amt_m0,fi_bill_amt_m1,fi_bill_amt_m2,fi_bill_amt_m3,fi_pmnt_mthd_nm_m0,fi_pmnt_mthd_nm_m1,fi_pmnt_mthd_nm_m2,fi_pmnt_mthd_nm_m3,fi_pmnt_amt_m0,fi_pmnt_amt_m1,fi_pmnt_amt_m2,fi_pmnt_amt_m3,fi_outst_age,fi_outst_amt,fi_outst_amt_curr,fi_outst_amt_30,fi_outst_amt_60,fi_outst_amt_90,fi_outst_amt_more_90,fi_outst_amt_write_off,fi_tot_mrc_amt_prev,fi_vo_mrc_amt_prev,fi_bb_mrc_amt_prev,fi_tv_mrc_amt_prev,fi_tot_mrc_qty_prev,fi_vo_mrc_qty_prev,fi_bb_mrc_qty_prev,fi_tv_mrc_qty_prev,fi_tot_dscnt_amt_prev,fi_tot_dscnt_qty_prev,fi_vo_dscnt_amt_prev,fi_vo_dscnt_qty_prev,fi_bb_dscnt_amt_prev,fi_bb_dscnt_qty_prev,fi_tv_dscnt_amt_prev,fi_tv_dscnt_qty_prev,fi_vo_rntl_prcnt_prev,fi_bb_rntl_prcnt_prev,fi_tv_rntl_prcnt_prev,fi_tot_srv_chrg_amt_prev,fi_vo_srv_chrg_amt_prev,fi_bb_srv_chrg_amt_prev,fi_tv_srv_chrg_amt_prev,fi_tot_srv_chrg_qty_prev,fi_vo_srv_chrg_qty_prev,fi_bb_srv_chrg_qty_prev,fi_tv_srv_chrg_qty_prev,nr_short_node,nr_long_node,nr_terminal,nr_minibox,nr_cable,nr_tel_center,nr_odfx,nr_fdh,nr_fdp,nr_ont,nr_tv_stb_free_qty,nr_tv_stb_qty,nr_max_down,nr_bb_mac,nr_prjct_dt,nr_prjct_typ,nr_suit_fr_dsl,pd_mix_cd,pd_mix_nm,pd_vo_prod_cd,pd_vo_prod_nm,pd_vo_fmly,pd_vo_sbfmly,pd_vo_accs_media,pd_vo_tech,pd_vo_prd_inst_date,pd_bb_prod_cd,pd_bb_prod_nm,pd_bb_fmly,pd_bb_sbfmly,pd_bb_accs_media,pd_bb_tech,pd_bb_speed,pd_bb_prd_inst_date,pd_tv_prod_cd,pd_tv_prod_nm,pd_tv_fmly,pd_tv_sbfmly,pd_tv_accs_media,pd_tv_tech,pd_tv_prd_inst_date,pd_vo_prod_cd_prev,pd_vo_prod_nm_prev,pd_vo_accs_media_prev,pd_vo_tech_prev,pd_bb_prod_cd_prev,pd_bb_prod_nm_prev,pd_bb_accs_media_prev,pd_bb_tech_prev,pd_tv_prod_cd_prev,pd_tv_prod_nm_prev,pd_tv_accs_media_prev,pd_tv_tech_prev,pd_vo_prd_inst_date_prev,pd_bb_prd_inst_date_prev,pd_tv_prd_inst_date_prev,pd_vo_fmly_prev,pd_vo_sbfmly_prev,pd_bb_fmly_prev,pd_bb_sbfmly_prev,pd_tv_fmly_prev,pd_tv_sbfmly_prev,pd_bb_speed_prev,pd_mix_cd_prev,pd_mix_nm_prev,srv_phy_street,srv_phy_neighborhood,srv_phy_city,srv_phy_county,srv_phy_province,srv_phy_project,srv_phy_vdp,srv_phy_addr,srv_curr_vo_qty_accs_mthd,srv_curr_bb_qty_accs_mthd,srv_curr_tv_qty_accs_mthd,srv_vo_qty_accs_mthd_m1,srv_bb_qty_accs_mthd_m1,srv_tv_qty_accs_mthd_m1,srv_vo_qty_accs_mthd_m2,srv_bb_qty_accs_mthd_m2,srv_tv_qty_accs_mthd_m2,srv_vo_qty_accs_mthd_m3,srv_bb_qty_accs_mthd_m3,srv_tv_qty_accs_mthd_m3,org_cntry,load_dt,mo_key,day_key,fi_bill_dt_m0,fi_bill_dt_m1,fi_bill_dt_m2,fi_bill_dt_m3,fi_bill_due_dt_m0,fi_bill_due_dt_m1,fi_bill_due_dt_m2,fi_bill_due_dt_m3,fi_bill_pmnt_dt_m0,fi_bill_pmnt_dt_m1,fi_bill_pmnt_dt_m2,fi_bill_pmnt_dt_m3,fi_bill_clr_dt_m0,fi_bill_clr_dt_m1,fi_bill_clr_dt_m2,fi_bill_clr_dt_m3,act_flowid_mail,act_flowid_key,act_flowid_dt,cntry_id,fmc_flag,fmc_org_cntry,fmc_household_id,fmc_start_date,fmc_end_date,fmc_status,fmc_status_change_date,bundle_code,bundle_name,bundle_inst_date,bundle_cease_date,fmc_sales_channel_type,fmc_sales_rep_id,service_id,bundle_code_prev,bundle_name_prev,bundle_inst_date_prev,bundle_cease_date_prev,fmc_reporting_action,fmc_vo_prod_cd_prev,fmc_vo_prod_nm_prev,fmc_vo_prod_inst_date,fmc_tv_prod_cd_prev,fmc_tv_prod_nm_prev,fmc_tv_prod_inst_date_prev,fmc_bb_prod_cd_prev,fmc_bb_prod_nm_prev,fmc_bb_prd_inst_date_prev,fmc_vo_tech_prev,fmc_bb_tech_prev,fmc_tv_tech_prev,fmc_bb_speed_prev,fmc_vo_tech,fmc_bb_tech,fmc_tv_tech,fi_vo_mrc_amt_prev_fmc,fi_bb_mrc_amt_prev_fmc,fi_tv_mrc_amt_prev_fmc,fmc_load_date,fmc_dt_yr_mth,pd_min_inst_dt_curr,pd_min_inst_dt_yr_mth,pd_mix_cd_prev_fmc,pd_mix_nm_prev_fmc,pd_min_inst_dt_prev,fi_tot_mrc_amt_prev_fmc,fmc_reporting_action_level_1,fmc_reporting_action_level_2,mo_tot_mrc_amt_prev,mo_tot_mrc_amt,fi_outst_amt_4_mths,fi_outst_amt_5_mths,fi_outst_amt_6_mths,fi_outst_amt_7_mths,fi_outst_amt_8_mths,fi_outst_amt_9_mths,fi_outst_amt_10_mths,fi_outst_amt_11_mths,fi_outst_amt_12_mths,fi_outst_amt_12_mths_plus,fi_overdue_age,fi_overdue_amt_1_mth,fi_overdue_amt_2_mths,fi_overdue_amt_3_mths,fi_overdue_amt_4_mths,fi_overdue_amt_5_mths,fi_overdue_amt_6_mths,fi_overdue_amt_7_mths,fi_overdue_amt_8_mths,fi_overdue_amt_9_mths,fi_overdue_amt_10_mths,fi_overdue_amt_11_mths,fi_overdue_amt_12_mths,fi_overdue_amt_12_mths_plus,fi_tot_srv_chrg_cnt,fi_tot_srv_chrg_cnt_neg,fi_tot_srv_chrg_cnt_pos,fmc_sales_channel_id,acct_stat_prev,bar_cnt_6mths,bb_cpe,bill_day,bills_cnt_6mths,bounced_paym_cnt_6mths,days_at_acct_stat_curr,days_at_acct_stat_prev,default_bill_cnt_6mths,default_cnt_6mths,direct_debit_flag,disputes_cnt_6mths,no_overdue_bills,churn_score_dt,churninv_score,churininv_centil,lgrossmarg,lgrossmarg_centil,oldest_unpaid_bill_dt,oldest_unpaid_due_dt,paym_dt_vs_due_dt_3ma,paym_plan_flag,promise_to_pay_cnt_6mths,srv_curr_mo_qty_accs_mthd,srv_mo_qty_accs_mthd_m1,tos_cnt_6mths,total_charges_curr,total_charges_prev,total_credit_amt,total_deposit_amt,total_dispute_amt,total_nrc_amt,total_nrc_amt_prev,total_overdue_amt,tv_cpe,act_self_install_flg,dt
 FROM
"db-analytics-prod"."fixed_cwp"
--WHERE act_cust_typ_nm = 'Residencial'
--AND act_acct_typ_grp ='MAS MOVIL'
)
,
DEV_PROD_JOIN as(
SELECT *
FROM UsefulDays_DEV UNION ALL
SELECT * FROM UsefulDays_PROD)
select * from dev_prod_join
