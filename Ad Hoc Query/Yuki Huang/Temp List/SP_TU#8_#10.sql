-- DROP PROCEDURE IF EXISTS temp.DM_Respond_Report_TU8_10;
-- CREATE PROCEDURE temp.`DM_Respond_Report_TU8_10`()
BEGIN


################TU8
Drop temporary table if exists temp.HB_info8;
Create temporary table if not exists temp.HB_info8 ( INDEX(affid) ) 
as (
select a.lead_sequence_id, 
a.affid,
a.state,
a.netmonthly as HB_NMI,
(select ums.description from jaglms.uw_master_streams ums where a.uw_stream = ums.master_stream_id limit 1) as UWStreamName,
a.decision as interim_decision,
if(sum(if(a.decision='ACCEPT',1,0))>0,'ACCEPT', 'REJECT') as Decision,
if(sum(if(a.decision='ACCEPT',1,0))>0,'>A<', a.decision_detail) as Decision_Detail,
coalesce(a.lead_provider_id, a.lead_source_id) as CampaignId,
a.campaign_name,
a.lead_source_id,
a.uw_cost,
min(a.insert_date) as first_insert_date,
max(a.insert_date) as recent_insert_date,
if(affid like 'MKT%', 'TU#8_MKT', if( affid like 'MTK%', 'TU#8_MTK', if(affid like 'MRKT%', 'TU#8_MRKT_Remail', ''))) as Applied_DM_Name,
email,
cf.`/x/cf/cf-score` AS CF_Score,  
cbb.`/x/cbb/cbb-score2` AS CBB_Score2

from 
datawork.mk_application a
JOIN jaglms.lead_source ls on a.lead_provider_id=ls.lead_source_id AND ls.master_source_id=25
LEFT OUTER JOIN datawork.mk_clearfraud cf ON a.Lead_Sequence_ID = cf.lead_sequence_id    
LEFT OUTER JOIN datawork.mk_clearbankbehavior cbb ON a.Lead_Sequence_ID = cbb.lead_sequence_id
where
(a.email not like '%moneykey%'or a.email not like'%mk.com')
and 
(a.affid like 'MKT%' or a.affid like 'MTK%' or a.affid like 'MRKT%')
and a.insert_date>='2019-05-01'
and a.insert_date<='2019-07-15'
group by a.affid);


Drop temporary table if exists temp.HB_info88;
Create temporary table if not exists temp.HB_info88 ( INDEX(affid) ) 
as (
select 
      if(hb.Decision='ACCEPT', a.lead_sequence_id, hb.lead_sequence_id) as lead_sequence_id, 
      hb.affid, hb.state, hb.HB_NMI, hb.UWStreamName, hb.Decision, hb.Decision_Detail, hb.campaign_name,
      hb.first_insert_date, 
      hb.recent_insert_date,
      hb.Applied_DM_Name,
      hb.CF_Score,
      hb.CBB_Score2,
      hb.uw_cost
from temp.HB_info7 hb
left join datawork.mk_application a on hb.affid=a.affid and hb.Decision='ACCEPT' and a.decision='ACCEPT' and a.insert_date between'2019-05-01' and '2019-07-15'
);



SET SQL_BIG_SELECTS=1;


Drop temporary table if exists TU8_mail;
Create temporary table if not exists TU8_mail 
as (select    'Mail' as Group_Split,
              y.*, 
              h.lead_sequence_id, 
              if(h.lead_sequence_id is not null, 1, 0) as Is_Applied, 
              h.Decision_Detail as Decision_Detail,
              if(h.decision='Accept', 1,0) as Is_Accepted, 
              h.campaign_name as HB_campaign_name, 
              h.first_insert_date as first_insert_date, 
              h.recent_insert_date as recent_insert_date,
              h.Applied_DM_Name as Applied_DM_Name,
              h.CF_Score as CF_Score,
              h.CBB_Score2 as CBB_Score2, 
              h.uw_cost,
              h.HB_NMI as  HB_NMI
              , h.affid as applied_affid
              ,n.lms_code as lms_code, 
              n.product as product, 
              n.state as lms_state, 
              
              n.pay_frequency,
              n.application_approved_amount,
              n.originated_loan_amount,
              
              n.campaign_name as LMS_campaign_name, 
              n.nc_lms_accepts as nc_lms_accepts, 
              n.is_originated as is_originated, 
              n.withdrawn_reason as withdrawn_reason, 
              n.net_monthly_income_update as net_monthly_income_update, 
              n.current_net_monthly_income as current_net_monthly_income, 
              n.`1st_payment_debit_date` as `1st_payment_debit_date`,
              n.is_1st_payment_debited as is_1st_payment_debited, 
              n.is_1st_payment_defaulted as is_1st_payment_defaulted
      from reporting.direct_mail_transunion y
      left join temp.HB_info77 h on h.affid=y.promotion_code and h.state=y.State
      left join reporting.AFR_Normal n on h.lead_sequence_id=n.lead_sequence_id and n.original_lead_received_date between '2019-05-01' and '2019-07-15'
      where y.campaign_name='Transunion 8' 
      group by y.ID);


Drop temporary table if exists TU8_remail;
Create temporary table if not exists TU8_remail 
as (select    'Remail' as Group_Split,
              y.*, 
              h.lead_sequence_id, 
              if(h.lead_sequence_id is not null, 1, 0) as Is_Applied, 
              h.Decision_Detail as Decision_Detail,
              if(h.decision='Accept', 1,0) as Is_Accepted, 
              h.campaign_name as HB_campaign_name, 
              h.first_insert_date as first_insert_date, 
              h.recent_insert_date as recent_insert_date,
              h.Applied_DM_Name as Applied_DM_Name,
              h.CF_Score as CF_Score,
              h.CBB_Score2 as CBB_Score2, 
              h.uw_cost,
              h.HB_NMI as  HB_NMI
              , h.affid as applied_affid
              ,n.lms_code as lms_code, 
              n.product as product, 
              n.state as lms_state, 
              n.pay_frequency,
              n.application_approved_amount,
              n.originated_loan_amount,
              n.campaign_name as LMS_campaign_name, 
              n.nc_lms_accepts as nc_lms_accepts, 
              n.is_originated as is_originated, 
              n.withdrawn_reason as withdrawn_reason, 
              n.net_monthly_income_update as net_monthly_income_update, 
              n.current_net_monthly_income as current_net_monthly_income, 
              n.`1st_payment_debit_date` as `1st_payment_debit_date`,
              n.is_1st_payment_debited as is_1st_payment_debited, 
              n.is_1st_payment_defaulted as is_1st_payment_defaulted
      from reporting.direct_mail_transunion y
      left join temp.HB_info77 h on h.affid=y.promotion_code_remail and h.state=y.State
      left join reporting.AFR_Normal n on h.lead_sequence_id=n.lead_sequence_id and n.original_lead_received_date between'2019-05-01' and '2019-07-15'
      where y.campaign_name='Transunion 8' and y.is_remail=1  
      group by y.ID);   



##########################TU9

Drop temporary table if exists temp.HB_info9;
Create temporary table if not exists temp.HB_info9 ( INDEX(affid) ) 
as (
select a.lead_sequence_id, 
a.affid,
a.state,
a.netmonthly as HB_NMI,
(select ums.description from jaglms.uw_master_streams ums where a.uw_stream = ums.master_stream_id limit 1) as UWStreamName,
a.decision as interim_decision,
if(sum(if(a.decision='ACCEPT',1,0))>0,'ACCEPT', 'REJECT') as Decision,
if(sum(if(a.decision='ACCEPT',1,0))>0,'>A<', a.decision_detail) as Decision_Detail,
coalesce(a.lead_provider_id, a.lead_source_id) as CampaignId,
a.campaign_name,
a.lead_source_id,
a.uw_cost,
min(a.insert_date) as first_insert_date,
max(a.insert_date) as recent_insert_date,
if(affid like 'MNU%', 'TU#9',if(affid like 'MRU%', 'TU#9_Remail', '')) as Applied_DM_Name,
email,
cf.`/x/cf/cf-score` AS CF_Score,  
cbb.`/x/cbb/cbb-score2` AS CBB_Score2

from 
datawork.mk_application a
JOIN jaglms.lead_source ls on a.lead_provider_id=ls.lead_source_id AND ls.master_source_id=25
LEFT OUTER JOIN datawork.mk_clearfraud cf ON a.Lead_Sequence_ID = cf.lead_sequence_id    
LEFT OUTER JOIN datawork.mk_clearbankbehavior cbb ON a.Lead_Sequence_ID = cbb.lead_sequence_id
where
(a.email not like '%moneykey%'or a.email not like'%mk.com')
and 
(a.affid like 'MNU%' or a.affid like 'MRU%')
and a.insert_date>='2019-07-04'
and a.insert_date<='2019-09-23'
group by a.affid);


Drop temporary table if exists temp.HB_info88;
Create temporary table if not exists temp.HB_info88 ( INDEX(affid) ) 
as (
select 
      if(hb.Decision='ACCEPT', a.lead_sequence_id, hb.lead_sequence_id) as lead_sequence_id, 
      hb.affid, hb.state, hb.HB_NMI, hb.UWStreamName, hb.Decision, hb.Decision_Detail, hb.campaign_name,
      hb.first_insert_date, 
      hb.recent_insert_date,
      hb.Applied_DM_Name,
      hb.CF_Score,
      hb.CBB_Score2,
      hb.uw_cost
from temp.HB_info7 hb
left join datawork.mk_application a on hb.affid=a.affid and hb.Decision='ACCEPT' and a.decision='ACCEPT' and a.insert_date between'2019-07-04' and '2019-09-23'
);



SET SQL_BIG_SELECTS=1;


Drop temporary table if exists TU9_mail;
Create temporary table if not exists TU9_mail 
as (select    'Mail' as Group_Split,
              y.*, 
              h.lead_sequence_id, 
              if(h.lead_sequence_id is not null, 1, 0) as Is_Applied, 
              h.Decision_Detail as Decision_Detail,
              if(h.decision='Accept', 1,0) as Is_Accepted, 
              h.campaign_name as HB_campaign_name, 
              h.first_insert_date as first_insert_date, 
              h.recent_insert_date as recent_insert_date,
              h.Applied_DM_Name as Applied_DM_Name,
              h.CF_Score as CF_Score,
              h.CBB_Score2 as CBB_Score2, 
              h.uw_cost,
              h.HB_NMI as  HB_NMI
              , h.affid as applied_affid
              ,n.lms_code as lms_code, 
              n.product as product, 
              n.state as lms_state, 
              
              n.pay_frequency,
              n.application_approved_amount,
              n.originated_loan_amount,
              
              n.campaign_name as LMS_campaign_name, 
              n.nc_lms_accepts as nc_lms_accepts, 
              n.is_originated as is_originated, 
              n.withdrawn_reason as withdrawn_reason, 
              n.net_monthly_income_update as net_monthly_income_update, 
              n.current_net_monthly_income as current_net_monthly_income, 
              n.`1st_payment_debit_date` as `1st_payment_debit_date`,
              n.is_1st_payment_debited as is_1st_payment_debited, 
              n.is_1st_payment_defaulted as is_1st_payment_defaulted
      from reporting.direct_mail_transunion y
      left join temp.HB_info77 h on h.affid=y.promotion_code and h.state=y.State
      left join reporting.AFR_Normal n on h.lead_sequence_id=n.lead_sequence_id and n.original_lead_received_date between '2019-07-04' and '2019-09-23'
      where y.campaign_name='Transunion 9' 
      group by y.ID);


Drop temporary table if exists TU8_remail;
Create temporary table if not exists TU8_remail 
as (select    'Remail' as Group_Split,
              y.*, 
              h.lead_sequence_id, 
              if(h.lead_sequence_id is not null, 1, 0) as Is_Applied, 
              h.Decision_Detail as Decision_Detail,
              if(h.decision='Accept', 1,0) as Is_Accepted, 
              h.campaign_name as HB_campaign_name, 
              h.first_insert_date as first_insert_date, 
              h.recent_insert_date as recent_insert_date,
              h.Applied_DM_Name as Applied_DM_Name,
              h.CF_Score as CF_Score,
              h.CBB_Score2 as CBB_Score2, 
              h.uw_cost,
              h.HB_NMI as  HB_NMI
              , h.affid as applied_affid
              ,n.lms_code as lms_code, 
              n.product as product, 
              n.state as lms_state, 
              n.pay_frequency,
              n.application_approved_amount,
              n.originated_loan_amount,
              n.campaign_name as LMS_campaign_name, 
              n.nc_lms_accepts as nc_lms_accepts, 
              n.is_originated as is_originated, 
              n.withdrawn_reason as withdrawn_reason, 
              n.net_monthly_income_update as net_monthly_income_update, 
              n.current_net_monthly_income as current_net_monthly_income, 
              n.`1st_payment_debit_date` as `1st_payment_debit_date`,
              n.is_1st_payment_debited as is_1st_payment_debited, 
              n.is_1st_payment_defaulted as is_1st_payment_defaulted
      from reporting.direct_mail_transunion y
      left join temp.HB_info77 h on h.affid=y.promotion_code_remail and h.state=y.State
      left join reporting.AFR_Normal n on h.lead_sequence_id=n.lead_sequence_id and n.original_lead_received_date between'2019-07-04' and '2019-09-23'
      where y.campaign_name='Transunion 9' and y.is_remail=1  
      group by y.ID);   



#######TU10

Drop temporary table if exists temp.HB_info10;
Create temporary table if not exists temp.HB_info10 ( INDEX(affid) ) 
as (
select a.lead_sequence_id, 
a.affid,
a.state,
a.netmonthly as HB_NMI,
(select ums.description from jaglms.uw_master_streams ums where a.uw_stream = ums.master_stream_id limit 1) as UWStreamName,
a.decision as interim_decision,
if(sum(if(a.decision='ACCEPT',1,0))>0,'ACCEPT', 'REJECT') as Decision,
if(sum(if(a.decision='ACCEPT',1,0))>0,'>A<', a.decision_detail) as Decision_Detail,
coalesce(a.lead_provider_id, a.lead_source_id) as CampaignId,
a.campaign_name,
a.lead_source_id,
a.uw_cost,
min(a.insert_date) as first_insert_date,
max(a.insert_date) as recent_insert_date,
if(affid like 'MNV%', 'TU#10',if(affid like 'MRV%', 'TU#10_Remail', '')) as Applied_DM_Name,
email,
cf.`/x/cf/cf-score` AS CF_Score,  
cbb.`/x/cbb/cbb-score2` AS CBB_Score2

from 
datawork.mk_application a
JOIN jaglms.lead_source ls on a.lead_provider_id=ls.lead_source_id AND ls.master_source_id=25
LEFT OUTER JOIN datawork.mk_clearfraud cf ON a.Lead_Sequence_ID = cf.lead_sequence_id    
LEFT OUTER JOIN datawork.mk_clearbankbehavior cbb ON a.Lead_Sequence_ID = cbb.lead_sequence_id
where
(a.email not like '%moneykey%'or a.email not like'%mk.com')
and 
(a.affid like 'MNV%' or a.affid like 'MRV%')
and a.insert_date>='2019-08-26'
and a.insert_date<='2019-11-18'
group by a.affid);


Drop temporary table if exists temp.HB_info88;
Create temporary table if not exists temp.HB_info88 ( INDEX(affid) ) 
as (
select 
      if(hb.Decision='ACCEPT', a.lead_sequence_id, hb.lead_sequence_id) as lead_sequence_id, 
      hb.affid, hb.state, hb.HB_NMI, hb.UWStreamName, hb.Decision, hb.Decision_Detail, hb.campaign_name,
      hb.first_insert_date, 
      hb.recent_insert_date,
      hb.Applied_DM_Name,
      hb.CF_Score,
      hb.CBB_Score2,
      hb.uw_cost
from temp.HB_info7 hb
left join datawork.mk_application a on hb.affid=a.affid and hb.Decision='ACCEPT' and a.decision='ACCEPT' and a.insert_date between'2019-08-26' and '2019-11-18'
);



SET SQL_BIG_SELECTS=1;


Drop temporary table if exists TU10_mail;
Create temporary table if not exists TU10_mail 
as (select    'Mail' as Group_Split,
              y.*, 
              h.lead_sequence_id, 
              if(h.lead_sequence_id is not null, 1, 0) as Is_Applied, 
              h.Decision_Detail as Decision_Detail,
              if(h.decision='Accept', 1,0) as Is_Accepted, 
              h.campaign_name as HB_campaign_name, 
              h.first_insert_date as first_insert_date, 
              h.recent_insert_date as recent_insert_date,
              h.Applied_DM_Name as Applied_DM_Name,
              h.CF_Score as CF_Score,
              h.CBB_Score2 as CBB_Score2, 
              h.uw_cost,
              h.HB_NMI as  HB_NMI
              , h.affid as applied_affid
              ,n.lms_code as lms_code, 
              n.product as product, 
              n.state as lms_state, 
              
              n.pay_frequency,
              n.application_approved_amount,
              n.originated_loan_amount,
              
              n.campaign_name as LMS_campaign_name, 
              n.nc_lms_accepts as nc_lms_accepts, 
              n.is_originated as is_originated, 
              n.withdrawn_reason as withdrawn_reason, 
              n.net_monthly_income_update as net_monthly_income_update, 
              n.current_net_monthly_income as current_net_monthly_income, 
              n.`1st_payment_debit_date` as `1st_payment_debit_date`,
              n.is_1st_payment_debited as is_1st_payment_debited, 
              n.is_1st_payment_defaulted as is_1st_payment_defaulted
      from reporting.direct_mail_transunion y
      left join temp.HB_info77 h on h.affid=y.promotion_code and h.state=y.State
      left join reporting.AFR_Normal n on h.lead_sequence_id=n.lead_sequence_id and n.original_lead_received_date between '2019-08-26' and '2019-11-18'
      where y.campaign_name='Transunion 10' 
      group by y.ID);


Drop temporary table if exists TU8_remail;
Create temporary table if not exists TU8_remail 
as (select    'Remail' as Group_Split,
              y.*, 
              h.lead_sequence_id, 
              if(h.lead_sequence_id is not null, 1, 0) as Is_Applied, 
              h.Decision_Detail as Decision_Detail,
              if(h.decision='Accept', 1,0) as Is_Accepted, 
              h.campaign_name as HB_campaign_name, 
              h.first_insert_date as first_insert_date, 
              h.recent_insert_date as recent_insert_date,
              h.Applied_DM_Name as Applied_DM_Name,
              h.CF_Score as CF_Score,
              h.CBB_Score2 as CBB_Score2, 
              h.uw_cost,
              h.HB_NMI as  HB_NMI
              , h.affid as applied_affid
              ,n.lms_code as lms_code, 
              n.product as product, 
              n.state as lms_state, 
              n.pay_frequency,
              n.application_approved_amount,
              n.originated_loan_amount,
              n.campaign_name as LMS_campaign_name, 
              n.nc_lms_accepts as nc_lms_accepts, 
              n.is_originated as is_originated, 
              n.withdrawn_reason as withdrawn_reason, 
              n.net_monthly_income_update as net_monthly_income_update, 
              n.current_net_monthly_income as current_net_monthly_income, 
              n.`1st_payment_debit_date` as `1st_payment_debit_date`,
              n.is_1st_payment_debited as is_1st_payment_debited, 
              n.is_1st_payment_defaulted as is_1st_payment_defaulted
      from reporting.direct_mail_transunion y
      left join temp.HB_info77 h on h.affid=y.promotion_code_remail and h.state=y.State
      left join reporting.AFR_Normal n on h.lead_sequence_id=n.lead_sequence_id and n.original_lead_received_date between'2019-08-26' and '2019-11-18'
      where y.campaign_name='Transunion 10' and y.is_remail=1  
      group by y.ID);   