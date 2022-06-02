WITH FMC_Table AS
( SELECT * FROM
"lla_cco_int_stg"."cwp_sp3_basekpis_dashboardinput_dinamico_rj")

/*SELECT fixedmonth, count(distinct fixedaccount) from FMC_TABLE
WHERE  f_activeeom = 1
group by 1 order by 1*/
,
-----New Customers-----
NEW_CUSTOMERS as (
Select 
act_acct_cd,load_dt, DATE_TRUNC('MONTH',CAST(load_dt AS DATE)) AS month_load,DATE_TRUNC('MONTH',CAST(act_cust_strt_dt AS DATE)) AS month_start,CAST(SUBSTR(pd_mix_cd,1,1) AS INT) AS n_rgu, max(act_acct_inst_dt) as act_acct_inst_dt ,max(act_cust_strt_dt) as act_cust_strt_dt,  DATE_DIFF ('DAY',CAST (max(act_cust_strt_dt) AS DATE),CAST (max(act_acct_inst_dt) AS DATE)) as Installation_lapse, 1 as NEW_CUSTOMER,
pd_bb_accs_media,pd_tv_accs_media,pd_vo_accs_media
from --"db-analytics-prod"."fixed_cwp"
"lla_cco_int_stg"."cwp_fix_union_dna"
WHERE act_cust_typ_nm = 'Residencial'
AND DATE_TRUNC('month',CAST(load_dt AS DATE)) = DATE_TRUNC('month',CAST(act_cust_strt_dt AS DATE))
GROUP BY act_acct_cd, load_dt, DATE_TRUNC('MONTH',CAST(load_dt AS DATE)),CAST(act_cust_strt_dt AS DATE),
CAST(SUBSTR(pd_mix_cd,1,1) AS INT), --max(act_acct_inst_dt) , max(act_cust_strt_dt) , 
--DATE_DIFF ('DAY',CAST (max(act_cust_strt_dt) AS DATE),CAST (max(act_acct_inst_dt) AS DATE)),
1, pd_bb_accs_media,pd_tv_accs_media,pd_vo_accs_media
)
,
New_Customers_FLAG as(
SELECT f.*, a.installation_lapse, a.new_customer, 
CASE when f.FIRST_SALES_CHNL_BOM is not null and f.FIRST_SALES_CHNL_EOM is not null then f.FIRST_SALES_CHNL_EOM
when f.FIRST_SALES_CHNL_BOM is null and f.FIRST_SALES_CHNL_EOM is not null then f.FIRST_SALES_CHNL_EOM
WHEN  f.FIRST_SALES_CHNL_EOM is null and f.FIRST_SALES_CHNL_BOM is not null then f.FIRST_SALES_CHNL_BOM
END as SALES_CHANNEL,
CASE WHEN a.act_acct_cd is not null then 1 else 0 end as monthsale_flag
 FROM FMC_TABLE AS f left join NEW_CUSTOMERS AS a
ON f.finalaccount = a.act_acct_cd and f.month = a.month_load)

--2,263,379
------------ INTERACTIONS ---------
,
clean_interaction_time as (
select *
FROM "db-stage-prod"."interactions_cwp"
    WHERE (cast(INTERACTION_START_TIME  as VARCHAR) != ' ')
    AND(INTERACTION_START_TIME IS NOT NULL)
    AND INTERACTION_ID NOT LIKE '%-%'
),
Interactions as (
    select *,
    CAST(SUBSTR(cast(INTERACTION_START_TIME as varchar),1,10) AS DATE) AS INTERACTION_DATE, DATE_TRUNC('month',CAST(SUBSTR(cast(INTERACTION_START_TIME as varchar),1,10) AS DATE)) AS month,
    CASE
    WHEN OTHER_INTERACTION_INFO4 Like '%Retencion%' THEN 'Retention'
    WHEN OTHER_INTERACTION_INFO4 Like 'Retencion ' THEN 'Retention'
    WHEN OTHER_INTERACTION_INFO4 Like 'RETENCION DE CLIENTES ' THEN 'Retention'
    WHEN OTHER_INTERACTION_INFO5 Like 'RETENCION DE CLIENTES' THEN 'Retention'
    WHEN INTERACTION_PURPOSE_DESCRIP = 'TRUCKROLL' or INTERACTION_PURPOSE_DESCRIP = 'TICKET' THEN 'Technical'
    WHEN INTERACTION_PURPOSE_DESCRIP = 'CLAIM'
        AND OTHER_INTERACTION_INFO5 IN (
        'ADSL Ethernet                                     ',
		'ADSL Smart Home                                   ',
		'ADSL WiFi                                         ',
		'Agrerar y Eliminar MAC                            ',
		'Ajuste                                            ',
		'Ajuste                                            ',
		'Ajuste                                            ',
		'Asignaciones                                      ',
		'Caller ID                                         ',
		'Cambio de Velocidad                               ',
		'Cita incumplida                                   ',
		'Conferencia                                       ',
		'Configuracion Correos                             ',
		'Correccion de Profile                             ',
		'Correo de voz                                     ',
		'DATOS                                             ',
		'Dano Masivo                                       ',
		'Danos                                             ',
		'Danos recurrentes                                 ',
		'Danos recurrentes                                 ',
		'Danos recurrentes                                 ',
		'Desac Correo Voz                                  ',
		'Despertador                                       ',
		'Dunning  Action                                   ',
		'Dunning  Action                                   ',
		'Dunning  Action                                   ',
		'Equipo recuperado                                 ',
		'Errores Cross Connection                          ',
		'FIS                                               ',
		'FSR DATOS E INTERNET                              ',
		'Fuera de meta                                     ',
		'INTERNET                                          ',
		'IP No Disponible                                  ',
		'IVR tiquetes                                      ',
		'IVR tiquetes                                      ',
		'Infructuosa temas Tec                             ',
		'Instalacion demora                                ',
		'Instalacion demora                                ',
		'Instalacion demora                                ',
		'Instalacion estetica                              ',
		'Instalacion estetica                              ',
		'Instalacion estetica                              ',
		'Instalacion incompleta                            ',
		'Instalacion incompleta                            ',
		'Instalacion incompleta                            ',
		'Instalaciones                                     ',
		'Intermitencia                                     ',
		'Intermitencia                                     ',
		'Intermitencia                                     ',
		'Internet                                          ',
		'Internet Cobre                                    ',
		'Internet HFC                                      ',
		'Lenguaje Audio Pixelacion                         ',
		'Lenguaje Audio Pixelacion                         ',
		'Lenguaje Audio Pixelacion                         ',
		'Linea Cobre                                       ',
		'Linea Digital                                     ',
		'Llamada en espera                                 ',
		'MASIVOS                                           ',
		'Mala atencion Area tecnica                        ',
		'Mala atencion Area tecnica                        ',
		'Mala atencion Area tecnica                        ',
		'Masivo                                            ',
		'Masivo                                            ',
		'Masivo                                            ',
		'No tiene servicio                                 ',
		'No tiene servicio                                 ',
		'No tiene servicio                                 ',
		'Numero en pantalla                                ',
		'Paquete de Seguridad                              ',
		'Plantillas Duplicadas                             ',
		'Programacion de                                   ',
		'QUEJA DE INFRAESTRUCTURA                          ',
		'RXC                                               ',
		'Reclamos                                          ',
		'Redes LAN y WAN                                   ',
		'Reparacion demora                                 ',
		'Reparacion demora                                 ',
		'Reparacion demora                                 ',
		'Reparacion estetica                               ',
		'Reparacion estetica                               ',
		'Reparacion estetica                               ',
		'Reparacion incompleta                             ',
		'Reparacion incompleta                             ',
		'Reparacion incompleta                             ',
		'Robo area tecnica                                 ',
		'Robo area tecnica                                 ',
		'Robo area tecnica                                 ',
		'Ruido Linea cruzada                               ',
		'Ruido Linea cruzada                               ',
		'Ruido Linea cruzada                               ',
		'Smart Home                                        ',
		'Smart Security                                    ',
		'Soporte Internet                                  ',
		'Soporte Linea                                     ',
		'Soporte TV                                        ',
		'TV DTH                                            ',
		'TV Master                                         ',
		'TV Motorola                                       ',
		'TV Nagra                                          ',
		'Transferencia                                     ',
		'WiFi                                              ',
		'WiFi                                              ',
		'WiFi                                              ',
		'DAÃ‘OS                                             ',
		'DAÃ‘OS                                             ',
		'DaÃ±o Masivo                                       ',
		'ESCALAMIENTO DAÃ‘OS/TECNICOS                       ',
		'DAï¿½OS   ',
		'Daï¿½o Masivo ',
		'DAï¿½OS',
		'Daï¿½o Masivo',
        'Escalamiento 2 Niv                                ',
        'Escalamiento 3er Nivel                            ',
        'Escalamiento Da os / T cnicos                    ',
        'Reclamos TV                                       ',
        'ACTIVACION DE PRODUCTO                            ',
        'Da o Masivo                                       ',
        'INSTALACION                                       ',
        'Quejas de Instal. F sicas                         ',
        'Verificaci n/ servicio no funciona                ',
        'Fuera de Meta                                     ',
        'IRREGULARIDAD EN SERVICIO                         ',
        'Dentro meta                                       ',
        'Dentro de Meta                                    ',
        'DAÑOS                                             '
        ) THEN 'Technical'

    WHEN OTHER_INTERACTION_INFO5 IN(
        'Consulta de saldo                                 ',
        'Cliente restringido                               ',
        'Consulta de reclamo                               ',
        'Consulta de cuentas                               ',
        'INFORMACION GENERAL                               ',
        'Cliente desconectado                              ',
        'Consulta de productos y servicios                 ',
        'Consulta Prorrateo                                ',
        'INTERNACIONALES (MADI)                            ',
        'IVR saldo                                         ',
        'NACIONALES                                        ',
        'LOCALES                                           ',
        'CELULARES                                         ',
        'Productos LDI                                     ',
        'Productos LDN                                     ',
        'Consulta de saldo                                 ',
        'IVR saldo                                         '
    ) THEN 'Account Info'

    WHEN OTHER_INTERACTION_INFO5 IN(
        'Facturacion                                       ',
        'Consulta arreglo de pago                          ',
        'CARGO NO APLICA                                   ',
        'TARIFA O CARGO FIJO                               ',
        'Facturacion                                       ',
        'Afiliacion Facturacion Web                        ',
        'Como pagar On Line                                ',
        'No entiende la Factura                            ',
        'PAGOS EN AGENCIA EXTERNA                          ',
        'DEVOLUCIONES DE CREDITO                           ',
        'Facturacion Web                                   ',
        'Pago no posteado                                  ',
        'CTA. FINAL IRREGULAR                              ',
        'Pagos  Recargas                                   ',
        'DESCUENTO DE JUBILADO                             ',
        'No entiende la Factura                            ',
        'PAGO CWP MAL REGISTRADO                           ',
        'INVEST. ALTO CONSUMO                              ',
        'Promociones o Precios                             ',
        'PAGOS EN AGENCIA EXTERNA                          ',
        'PROMOCIONES                                       ',
        'FSR RES.1ER.CONT.POSTPAGO                         ',
        'IMPRESION DE FACTURA                              ',
        'PLANES PROMOCIONALES                              ',
        'INVEST. ALTO CONSUMO                              ',
        'Pagos  Recargas                                   ',
        'TRANSFERENCIA DE SALDO                            ',
        'ANUNCIO DEL DIRECTO                               '      
    ) THEN 'Billing' 
    ELSE 'Others'
    END AS INTERACTION_TYPE
FROM clean_interaction_time
)

----------------------- Soft DX A9 and Never Pay A13----------------------
,
First_BILLING as (
Select 
act_acct_cd,DATE_TRUNC('MONTH',CAST(load_dt AS DATE)) AS month_load,DATE_TRUNC('MONTH',CAST(act_cust_strt_dt AS DATE)) AS month_start,pd_bb_accs_media,pd_tv_accs_media,pd_vo_accs_media, CONCAT(act_acct_cd, cast(first_value(fi_bill_dt_m0) ignore nulls over (partition by act_acct_cd order by load_dt) as varchar)) as First_Bill, 
fi_outst_age
from "lla_cco_int_stg"."cwp_fix_union_dna"
WHERE act_cust_typ_nm = 'Residencial'
and cast(load_dt as date)between date_trunc('Month',cast (act_cust_strt_dt as date))and date_add('DAY',60, date_trunc('Month',cast (act_cust_strt_dt as date))) AND 
FI_OUTST_AGE > 0 and fi_outst_age is not null
)
,
OVERDUE_DAYS_FIRST_BILL as(
SELECT
 date_trunc('Month',cast (act_cust_strt_dt as date)) as Sales_Month,DATE_TRUNC('MONTH',CAST(load_dt AS DATE)) AS month_load,first_value(fi_bill_dt_m0) ignore nulls over (partition by act_acct_cd order by load_dt) as First_Bill_DT,
    act_acct_cd, 
    act_acct_inst_dt,
   fi_outst_age,
   first_value(fi_bill_pmnt_dt_m0) ignore nulls over (partition by act_acct_cd order by load_dt) as first_bill_PMNT_date,
    CONCAT(act_acct_cd, cast(first_value(fi_bill_dt_m0) ignore nulls over (partition by act_acct_cd order by load_dt) as varchar)) as First_Bill_Overdue
    FROM "lla_cco_int_stg"."cwp_fix_union_dna"
)

,ALL_OVERDUE_DAYS AS(
SELECT *
FROM (SELECT * from OVERDUE_DAYS_FIRST_BILL
where first_bill_overdue in (select first_bill from first_billing))
)

,account_summary as(
   SELECT Sales_Month, Month_load,
        act_acct_cd,
        first_bill_PMNT_date, first_bill_dt,
        MAX(fi_outst_age) AS MAX_OUTS_AGE
    FROM all_overdue_days
    GROUP by sales_month, month_load, act_acct_cd, first_bill_PMNT_date, first_bill_dt
)
,sft_hard_dx as (
SELECT Sales_month,month_load, act_acct_cd AS act_acct_cd_dx,
--DATE_TRUNC('MONTH',CAST(load_dt AS DATE)) AS Month_dx,
CASE WHEN max_outs_age >=46 and first_bill_PMNT_date is null
then 1 else 0 END AS SOFT_DX,
CASE WHEN max_outs_age >= 90 and first_bill_PMNT_date is null 
then 1 else 0 end as HARD_DX
from account_summary
where month_load between date(first_bill_dt)and date_add('DAY',90, date(sales_month))
--WHERE fi_outst_age = 46 or fi_outst_age = 90
)

,Join_dx_new_customers as (
SELECT a.month_start, act_acct_cd, soft_dx, hard_dx
FROM New_customers AS a
LEFT JOIN sft_hard_dx AS b
ON a.act_acct_cd = b.act_acct_cd_dx
)

,FLAG_SOFT_HARD_DX as(
SELECT f.*, soft_dx, hard_dx,
CASE WHEN soft_dx = 1 THen 1 ELSE null End as STRAIGHT_SOFT_DX_FLAG,
CASE WHEN HARD_DX = 1 THen 1 ELSE null End as NEVER_PAID_FLAG
FROM NEW_CUSTOMERS_FLAG AS f left join Join_dx_new_customers AS a
ON f.finalaccount = a.act_acct_cd and f.month = a.month_start
)


-------------- LATE INSTALLATIONS B1 ------
, 

Late_installation_flag as(
Select*, 
CASE WHEN Installation_lapse > 5 then 1 else 0 END as late_inst_flag
From FLAG_SOFT_HARD_DX
)
--select count(*) from LATE_INSTALLATION_FLAG
,
Join_newcustomers_interactions as (
SELECT
b.account_id,
CASE WHEN account_id IS NOT NULL then 1 else 0 END as early_interaction_flag
FROM NEW_CUSTOMERS AS a
LEFT JOIN interactions AS b
ON a.act_acct_cd = b.account_id
WHERE DATE_DIFF ('DAY',CAST (act_acct_inst_dt AS DATE),CAST (interaction_date AS DATE)) <=21
AND interaction_type ='Technical'
GROUP BY account_id
)

------ EARLY INTERACTION B4 -----
,
NEW_CUSTOMER_INTERACTIONS_INFO as (
SELECT
a.act_acct_cd,b.account_id,a.month_start,
CASE WHEN account_id IS NOT NULL then 1 else 0 END as early_interaction_flag
FROM NEW_CUSTOMERS AS a
LEFT JOIN interactions AS b
ON a.act_acct_cd = b.account_id
WHERE DATE_DIFF ('DAY',CAST (act_acct_inst_dt AS DATE),CAST (interaction_date AS DATE)) <=21
AND interaction_type ='Technical'
GROUP BY 1,2,3
),

EARLY_INT_FLAG as(
SELECT f.*, early_interaction_flag
FROM Late_Installation_Flag AS f left join NEW_CUSTOMER_INTERACTIONS_INFO AS a
ON f.finalaccount = a.act_acct_cd and f.month = a.month_start)

----- EARLY TICKET B9 ----
,
EARLY_TICKET_INFO as (
SELECT
f.account_id, f.month,
CASE WHEN f.account_id IS NOT NULL then 1 else NULL END as early_ticket_flag
FROM NEW_CUSTOMERS AS e
inner JOIN interactions AS f
ON e.act_acct_cd = f.account_id
WHERE DATE_DIFF ('week',CAST (act_acct_inst_dt AS DATE),CAST (interaction_date AS DATE)) <=7
AND INTERACTION_PURPOSE_DESCRIP = 'TICKET'
GROUP BY account_id, month
)

,
EARLY_TKT_FLAG as (
SELECT f.*, early_ticket_flag
FROM EARLY_INT_FLAG AS f left join EARLY_TICKET_INFO AS a
ON f.finalaccount = a.account_id and f.month = a.month)
--Select count (distinct finalaccount) from early_tkt_flag where date(month) = date('2022-02-01') and early_ticket_flag = 1
--select *  from early_tkt_flag where early_ticket_flag is not null and date(month) = date('2022-02-01') order by early_ticket_flag desc
-------- Billing CLaims C6 -----
,
STOCK_KEY as (
Select A.* ,CONCAT(FinalAccount, SUBSTR(CAST(month AS varchar),1,7)) AS key_Stock
FROM EARLY_TKT_FLAG as A
order by finalaccount
)
,
customers_billing_claims as (
SELECT  CONCAT(ACCOUNT_ID, SUBSTR(CAST(DATE_TRUNC('Month',INTERACTION_DATE) AS VARCHAR),1,7)) as Key_bill_claims
FROM interactions
WHERE INTERACTION_TYPE = 'Billing'
GROUP BY CONCAT(ACCOUNT_ID, SUBSTR(CAST(DATE_TRUNC('Month',INTERACTION_DATE) AS VARCHAR),1,7))
)
,
Billing_claims_FLAG as (
SELECT
a.*,
CASE WHEN Key_bill_claims IS NOT NULL then 1 else 0 END as Bill_claim_flag
FROM STOCK_KEY AS a
LEFT JOIN customers_billing_claims AS b
ON a.key_stock = b.Key_bill_claims
)

----------- MRC Changes ----------
,
MRC_changes as (
SELECT  CONCAT(act_acct_cd, SUBSTR(CAST(DATE_TRUNC('Month',load_DT) AS VARCHAR),1,7)) as Key_MRC_changes
,((fi_tot_mrc_amt-fi_tot_mrc_amt_prev)/fi_tot_mrc_amt_prev)  AS MRC_Change
FROM  "lla_cco_int_stg"."cwp_fix_union_dna"
WHERE pd_vo_prod_nm_prev = pd_vo_prod_nm
AND pd_bb_prod_nm_prev = pd_BB_prod_nm
AND pd_tv_prod_nm_prev = pd_tv_prod_nm
group by CONCAT(act_acct_cd, SUBSTR(CAST(DATE_TRUNC('Month',load_DT) AS VARCHAR),1,7)),((fi_tot_mrc_amt-fi_tot_mrc_amt_prev)/fi_tot_mrc_amt_prev) 
)
,
Join_MRC_change as (
SELECT
a.*,
b.MRC_CHANGE
FROM Billing_claims_FLAG AS a
LEFT JOIN MRC_changes AS b
ON a.key_stock = b.Key_MRC_changes
)
,
Change_MRC_Flag AS (
SELECT *
 ,CASE WHEN MRC_change > 0.05 or MRC_change< -0.05 then 1 else 0 END as MRC_change_flag
FROM Join_MRC_change
)

----------------- Mounting Bills ------------
,
Overdue_records as (
select 
    date_trunc('Month',cast (dt as date)) as Month,
    act_acct_cd, 
    case when fi_outst_age is null then -1 else fi_outst_age end as fi_outst_age,
    case when fi_outst_age=60 --AND cast(fi_bill_amt_m0 as double) is not null and cast(fi_bill_amt_m0 as double) >0 
    then 1 else 0 end as day_60,
    first_value(case when fi_outst_age is null then -1 else fi_outst_age end) IGNORE NULLS over(partition by date_trunc('Month',cast (load_dt as date)),
    act_acct_cd order by load_dt desc) as last_overdue_record,
    first_value(case when fi_outst_age is null then -1 else fi_outst_age end) IGNORE NULLS over(partition by date_trunc('Month',cast (load_dt as date)),
    act_acct_cd order by load_dt) as first_overdue_record
from   "lla_cco_int_stg"."cwp_fix_union_dna"
where act_cust_typ_nm = 'Residencial' and cast(load_dt as date) between
date_trunc('MONTH', cast(load_dt as date)) and date_add('MONTH', 1, date_trunc('MONTH', cast(load_dt as date)))
--date_trunc('MONTH', cast(load_dt as date)) and date_add('MONTH', 1, date_trunc('MONTH', cast(load_dt as date)))

),

Grouped_Overdue_records as (
select Month, act_acct_cd,
max(fi_outst_age) as max_overdue,
max(day_60) as mounting_bill_flag,
max(last_overdue_record) as last_overdue_record,
max(first_overdue_record) as first_overdue_record
from Overdue_records
GROUP BY Month, act_acct_cd
)
,
Mounting_bills_Flag as(
select f.*, b.Mounting_bill_flag
from CHANGE_MRC_FLAG as f left join Grouped_Overdue_records as B
ON f.finalaccount = b.act_acct_cd AND f.Month = b.Month
)
,
ALL_FLAGS as(
SELECT * FROM Mounting_bills_Flag)

--XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX           XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX
--XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX  RESULTS  XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX
--XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX           XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX
/*SELECT MONTH, SALES_CHANNEL, count(distinct fixedaccount)
from all_flags where f_activeeom =1
Group by 1,2 order by 1,2*/
-------------- FLAGS TABLE -----------

--/*
,FullTable_KPIsFlags AS(
Select *, case when Monthsale_flag = 1 then concat(cast(Monthsale_flag as varchar), FixedAccount) else NULL end as F_SalesFlag, 
   case when STRAIGHT_SOFT_DX_FLAG = 1 and New_customer =1 then concat(cast(STRAIGHT_SOFT_DX_FLAG as varchar), FixedAccount) else NULL end as F_SoftDxFlag, 
    case when Never_Paid_Flag = 1 then concat(cast(Never_Paid_Flag as varchar), FixedAccount) else NULL end as F_NeverPaidFlag,
    case when Late_inst_flag = 1 and New_customer =1 then concat(cast(late_inst_flag as varchar), FixedAccount) else NULL end as F_LongInstallFlag,
    case when early_interaction_flag = 1 and New_customer =1 then concat(cast(early_interaction_flag as varchar), FixedAccount) else NULL end as F_EarlyInteractionFlag,
    case when early_ticket_flag = 1 and New_customer =1 then concat(cast(early_ticket_flag as varchar), FixedAccount) else NULL end as F_EarlyTicketFlag,
    case when Bill_claim_flag = 1 then concat(cast(Bill_claim_flag as varchar), FixedAccount) else NULL end as F_BillClaim,
    case when MRC_change_flag = 1 then concat(cast(MRC_change_flag as varchar), FixedAccount) else NULL end as F_MRCChange,
    case when Mounting_bill_flag = 1 then concat(cast(mounting_bill_flag as varchar), Fixedaccount) else NULL end as F_MountingBillFlag
From All_Flags
)
,SalesChannel_SO AS(
SELECT DISTINCT Month,Channel_desc,account_id
,CASE WHEN Channel_desc IN ('Provincia de Chiriqui','PROM','VTASE','PHs1','Busitos','Alianza','Coronado','Ventas Externas/ADSL','PHs 2') OR Channel_desc LIKE '%PROM%' OR Channel_desc LIKE '%VTASE%' OR Channel_desc LIKE '%Busitos%' OR Channel_desc LIKE '%Alianza%' THEN 'D2D (Own Sales force)'
    WHEN Channel_desc IN('Dinamo','Oficinista','Distribuidora Arandele','Orde Technology','SLAND','SI Panamá') THEN 'D2D (Outsourcing)'
    WHEN Channel_desc IN('Vendedores','Metro Mall','WESTLAND MALL','TELEMART AGUADULCE') THEN 'Retail (Own Stores)'
    WHEN Channel_desc IN(/*'Telefono',*/'123 Outbound','Gestión') OR Channel_desc LIKE '%Gestión%' OR Channel_desc LIKE '%Gestion%' THEN 'Outbound – TeleSales'
    WHEN Channel_desc IN('Centro de Retencion','Centro de Llamadas','Call Cnter MULTICALL') THEN 'Inbound – TeleSales'
    WHEN Channel_desc IN('Nestrix','Tienda OnLine','Live Person','Telefono') THEN 'Digital'
    WHEN Channel_desc IN('Panafoto Dorado','Agencia') OR Channel_desc LIKE '%Agencia%' OR Channel_desc LIKE '%AGENCIA%' THEN 'Retail (Distributer-Dealer)'
    WHEN Channel_desc IN('CIS+ GUI','Solo para uso de IT','Apuntate',' CU2Si','RC0E Collection','Carta','Proyecto','DE=Demo','Recarga saldo','Port Postventa','Feria','Administracion','Postventa-verif.orde','No Factibilidad','Orden a construir','Inversiones AP','Promotor','VIVI MAS') OR Channel_desc LIKE '%Feria%' THEN 'Not a Sales Channel'
END AS Sales_Channel_SO
FROM  ( SELECT DISTINCT date_trunc('Month', date(completed_date)) AS Month, account_id,first_value(channel_desc) over   (partition by account_id order by order_start_date) as channel_desc
        FROM "db-analytics-prod"."so_hdr_cwp" WHERE order_type ='INSTALLATION'
       )
)
,FullTable_Adj AS(
SELECT DISTINCT  f.*,Sales_Channel_SO
FROM FullTable_KPIsFlags f LEFT JOIN SalesChannel_SO s ON f.fixedaccount=cast(s.account_id as varchar)
--order by fixedaccount
)
--select Month, sum(early_ticket_flag), count(distinct F_EarlyTicketFlag) as Unique_EarlyTicket from fullTable_KPIsFLAGS group by 1 
------ RESULTS QUERY -----------------------

select distinct Month, B_Final_TechFlag, B_FMCSegment, E_Final_TechFlag, E_FMCSegment, b_final_tenure,e_final_tenure,B_FixedTenure,E_FixedTenure,count(distinct fixedaccount) as activebase, sum(monthsale_flag) as Sales, sum(STRAIGHT_SOFT_DX_FLAG) as Soft_Dx, sum (Never_Paid_Flag) as NeverPaid, sum(late_inst_flag) as Long_installs,sum(early_interaction_flag) as Early_Issues, sum(early_ticket_flag) as Early_ticket, count(distinct F_SalesFlag) as Unique_Sales, count(distinct F_SoftDxFlag) as Unique_SoftDx,
    count(distinct F_NeverPaidFlag) as Unique_NeverPaid,
    count(distinct F_LongInstallFlag) as Unique_LongInstall,
    count(distinct F_EarlyInteractionFlag) as Unique_EarlyInteraction,
    count(distinct F_EarlyTicketFlag) as Unique_EarlyTicket,
    count(distinct F_BillClaim) as Unique_BillClaim,
    count(distinct F_MRCChange) as Unique_MRCChange,
    count (distinct F_MountingBillFlag) as Unique_MountingBill,
    B_FMCTYPE, E_FMCTYPE, First_Sales_Chnl_EOM, First_sales_CHNL_BOM, Last_Sales_CHNL_EOM, Last_Sales_CHNL_BOM , SALES_CHANNEL,sales_channel_so
from FullTable_Adj
WHERE ((Fixedchurntype != 'Fixed Voluntary Churner' and Fixedchurntype != 'Fixed Involuntary Churner') or  Fixedchurntype is null) and finalchurnflag !='Fixed Churner'
Group by 1,2,3,4,5, B_FMCTYPE, E_FMCTYPE,First_Sales_Chnl_EOM, First_sales_CHNL_BOM, Last_Sales_CHNL_EOM, Last_Sales_CHNL_BOM, SALES_CHANNEL,sales_channel_so,b_final_tenure,e_final_tenure,B_FixedTenure,E_FixedTenure
--Order by 1 desc, 2,3,4,5, B_FMCTYPE, E_FMCTYPE,First_Sales_Chnl_EOM, First_sales_CHNL_BOM, Last_Sales_CHNL_EOM, Last_Sales_CHNL_BOM, SALES_CHANNEL,sales_channel_so
