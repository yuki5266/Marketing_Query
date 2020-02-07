select ma.lead_sequence_id,ma.email,ma.decision,ma.decision_detail,
ma.insert_date, lh.lead_sequence_id,lh.loan_header_id,la.lms_application_id,n.loan_application_id,
case
when lh.lead_sequence_id is null then 'NOT IN LMS'
when la.lms_application_id is null then 'NOT IN LA'
when n.loan_application_id is null then 'NOT IN AFR'
when lh.lead_sequence_id IS NOT NULL AND n.loan_application_id is NOT null AND la.lms_application_id IS NOT NULL then 'OKAY'
ELSE 'CHECKED'
END AS CHECKER1,
la.loan_sequence AS La_loan_sequence,
la.lms_customer_id,la.lms_code,
n.loan_sequence,n.nc_lms_accepts

from datawork.mk_application ma
left join jaglms.lms_loan_header lh on ma.lead_sequence_id = lh.lead_sequence_id
left join reporting.leads_accepted la on lh.loan_header_id = la.lms_application_id and la.lms_code='JAG'
left join reporting.AFR_Normal n on lh.loan_header_id=n.loan_application_id  and n.lms_code='JAG'
where date(ma.insert_date)='2019-12-02'
and ma.decision='Accept'
and ma.organization_id is null
and ma.email not like '%moneykey%';




select * from jaglms.lms_loan_header lh where lh.lead_sequence_id=2314382962;
select * from reporting.leads_accepted where emailaddress='duschsteven@gmail.com';
select  lead_sequence_id, loan_application_id, loan_sequence from reporting.AFR_Normal where email_address='duschsteven@gmail.com';


## HB ACCEPTED:1871 
## LA AND AFR NC ACCEPTED:1769
## LA AND AFR RC ACCEPTED:1852-1769=83  --1852 in la and in afr
## 19 RECORDS ACCEPTED IN HB BUT NOT IN JAG LMS


























#Email Marketing Queries
?
#datawork version
select
lms.organization_id,
ls.cost as bid,
a.lead_bid,
case when ls.cost<=10 then '$3-$10'
             when ls.cost>10 and ls.cost<=20 then '$11-$20' 
             when ls.cost>20 and ls.cost<=30 then '$21-$30'
             when ls.cost>30 and ls.cost<=40 then '$31-$40'
             when ls.cost>40  then '$41+'
             else null
end as lead_bid_group,
-- (select lms.organization_id from jaglms.lead_master_sources lms, jaglms.lead_source ls where ls.lead_source_id = a.lead_provider_id and ls.master_source_id = lms.master_source_id limit 1) as Organization_Id,
a.lead_sequence_id,
?
a.insert_date as received_time,
a.insert_date as TimeStampReceived,
date(a.insert_date) as DateReceived,
dayname(a.insert_date) as DayReceived,
hour(a.insert_date) as HourReceived,
monthname(a.insert_date) as MonthReceived,
month(a.insert_date) as MonthReceivedNum, 
?
(Select ls.tierkey from jaglms.lead_source ls where a.lead_provider_id=ls.lead_source_id limit 1) as tierkey_ls,
a.lead_provider_id as CampaignId,
(select ls.description from jaglms.lead_source ls where ls.lead_source_id = a.lead_provider_id limit 1) as CampaignName,
(case when (Select ls.description from jaglms.lead_source ls where a.lead_provider_id=ls.lead_source_id limit 1) like '%#%' then 1 else 0 end) as IsHashtagCampaign,
(select ls.master_source_id from jaglms.lead_source ls where ls.lead_source_id = a.lead_provider_id limit 1) as LeadProviderId,
(select lms.description from jaglms.lead_master_sources lms, jaglms.lead_source ls where ls.lead_source_id = a.lead_provider_id and ls.master_source_id = lms.master_source_id limit 1) as LeadProviderName,
a.affid,
a.subid,
?
a.email,
a.firstname,
a.lastname,
?
a.is_returning as is_returning_HB,
if(lms.organization_id=1,la.isreturning, lacf.isreturning) as is_returning_LMS,
(select (case when ld1.value = 'true' then 1 else 0 end) from jaglms.mk_lead_data30 ld1 where ld1.lead_sequence_id >= 2247111296 and ld1.lead_sequence_id = a.lead_sequence_id and ld1.var_name = 'RC_FROM_LEAD' limit 1) as Is_RC_From_Lead,
?
a.routingnumber as routingnumberHB, 
a.bankname as banknameHB,
if(lms.organization_id=1, la.aba, lacf.aba) as routingnumberLMS,
c.bankname as banknameLMS,
c.account_number as accountnumberLMS,
?
upper(a.state) as State,
a.bastion,
a.payfrequency as pay_frequency,
?
(case when a.decision = 'ACCEPT' then 1 else 0 end) as accepted,
(case when a.uw_cost > 0 or a.decision = 'ACCEPT' then 1 else 0 end) as IsUniqueLook,
?
a.uw_cost,
a.lead_cost,
a.uw_cost + a.lead_cost as marketing_cost,
a.`clarity-cost` as clarity_cost,
a.`datax-cost` as datax_cost,
?
a.`Business Rule Set Pass` as PreUWRuleSetName,
a.business_rule_set as PreUWRuleSetId,
(select ums.description from jaglms.uw_master_streams ums where a.uw_stream = ums.master_stream_id limit 1) as UWStreamName,
a.uw_stream as UWStreamId,
(select uw.postprocess_business_rules from jaglms.uw_streams uw, jaglms.uw_master_streams ums where a.uw_stream = ums.master_stream_id and ums.master_stream_id = uw.master_stream_id order by uw.stream_id desc limit 1) as PostUWRuleSetId,
(select brs.description from jaglms.business_rule_sets brs, jaglms.uw_streams uw, jaglms.uw_master_streams ums where a.uw_stream = ums.master_stream_id and ums.master_stream_id = uw.master_stream_id and uw.postprocess_business_rules = brs.business_rule_set_id order by uw.stream_id desc limit 1) as PostUWRuleSetName,
?
(case when a.`Business Rule Set Pass` = 'Blue Light Special' then 1 else 0 end) as 'Is_Blue_Light_Special',
(case when a.uw_stream = 25 then 1 else 0 end) as 'Is_FT_UW_Stream',
?
ifnull(wp.`/wp_pro/ipch_is_proxy`,'NONE') as 'WP_IsProxy',
?
(case
when a.decision = 'REJECT' and ci.`/xr/i/action` = 'Deny' then 'Clear Inquiry Reject'
when a.decision = 'REJECT' and a.REJECT = 'Internal Underwriting' then concat('IU: ', a.`Business Rule Reject`)
when a.decision = 'REJECT' and a.REJECT != 'Internal Underwriting' then a.REJECT
#when a.decision = 'REJECT' and a.RESULT like '%Schema%' then COALESCE(concat('EPIC Reject: ', (select lo.RejectReason from ais.vw_client c inner join ais.vw_loans lo on lo.DebtorClientId = c.Id inner join ais.vw_underwriting uw on uw.loanId = lo.id where c.EmailAddress = a.email and date(CONVERT_TZ(uw.TimeStamp, 'US/Central','US/Eastern')) >= date(l.date_added) limit 1)), 'EPIC Reject')
when a.decision = 'REJECT' and a.RESULT like '%Schema%' then 'EPIC Reject'
when a.decision = 'ACCEPT' then a.RULE_DESCRIPTION 
else a.decision_detail end) as Rule,
?
a.reject,
a.result,
a.decision_detail,
a.rule_description,
?
a.ipaddress,
a.routingnumber,
a.netmonthly as monthly_income,
a.orig_amt_requested,
a.requestedamount as requested_amount,
cf.`/x/cf/cf-score` as 'Clarity_Clear_Fraud_Score',
cbb.`/x/cbb/cbb-score` as 'Clarity_Clear_Bank_Behavior_Score',
cbb.`/x/cbb/cbb-score2` as 'Clarity_Clear_Bank_Behavior_Score2',
crh.`/xr/crh/srh/active-duty-status` as CRH_Active_Duty_Status,
a.ismilitary, 
?
am.mk_risk_score,
am.mk_conv_score,
a.empname, 
a.jobtitle, 
a.refurl, 
am.mk_nocontact_score,
am.RC_FROM_LEAD,
a.paytype,
a.incometype as income_source,
a.accounttype
?
from 
datawork.mk_application a
left join datawork.mk_clearinquiry ci on a.lead_sequence_id = ci.lead_sequence_id
left join datawork.mk_whitepages wp on a.lead_sequence_id = wp.lead_sequence_id
left join datawork.mk_clearfraud cf on a.lead_sequence_id = cf.lead_sequence_id
left join datawork.mk_clearbankbehavior cbb on a.lead_sequence_id = cbb.lead_sequence_id
left join reporting.leads_accepted la on a.lead_sequence_id = la.lead_sequence_id
?
left join reporting_cf.leads_accepted lacf on a.lead_sequence_id = lacf.lead_sequence_id
?
left join jaglms.lms_customer_info_flat c on a.customer_id = c.customer_id
left join datawork.mk_clearrecenthistory crh on a.lead_sequence_id = crh.lead_sequence_id
left join datawork.mk_datax d on a.lead_sequence_id = d.lead_sequence_id
left join datawork.mk_factortrust ft on a.lead_sequence_id = ft.lead_sequence_id
left join datawork.mk_application_more am on a.lead_sequence_id = am.lead_sequence_id
left join jaglms.lead_source ls on a.lead_provider_id = ls.lead_source_id
left join jaglms.lead_master_sources lms on ls.master_source_id = lms.master_source_id
?
where
a.decision = 'ACCEPT' and 
(a.lead_provider_id != 205 or a.lead_provider_id is null) and
ls.description not like '%test%' and
#a.lead_provider_name = "Avenue Link" and
ls.description='SFL-TX-VIP' and
#a.lead_provider_id = 315 or 413 or 239 or 615 or 616 and
#a.uw_cost > 0 and
#a.routingnumber = '026014902' and
#a.state = 'OH' and
#a.lead_sequence_id = '2169306773' and
#crh.`/xr/crh/srh/active-duty-status` is null and
#a.orig_amt_requested > 0 and #uncomment this line if you want to see only the leads with an orig_amt_requested
?
a.insert_date between '2019-07-03 00:00:00' and '2019-07-03 23:59:59' #modify date range here; don't make it too big because query will time out
-- and lms.organization_id=1
order by a.insert_date desc
;