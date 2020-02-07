
			DROP TEMPORARY TABLE IF EXISTS table1;
			CREATE TEMPORARY TABLE IF NOT EXISTS table1 ( INDEX(lead_provider_id,lead_source_id,master_source_id,lead_sequence_id,origination_loan_id) ) 
			AS (

select aa.* 
from (
select

la.product,
la.origination_loan_id,
ls.lead_source_id,
a.lead_sequence_id,
a.firstname, a.lastname, a.email, a.insert_date received_time,  
CASE WHEN length(a.state)>2 or a.state is null THEN 'X-NotAvail'
     ELSE a.state
     END AS STATE,
if(lms.master_source_id=25, 'ORG', 'EXT') AS Channel,
ls.organic_group,
ls.organic_sub_group,
ls.master_source_id, 
lms.description as lead_vendor, 
a.lead_provider_id,
ls.description as lead_campaign_name,
1 as IsLook,
if(a.uw_cost>0 or a.decision='ACCEPT', 1, 0) as IsUniqueLook,
if(a.decision='ACCEPT',1,0) as IsAccepted, 
if(la.origination_loan_id is null, 0, 1) as IsOriginated, ##

case 
    when a.decision = 'REJECT' and a.decision_detail = 'Internal Underwriting' then 'Internal_U/W'
        when a.decision = 'REJECT' and a.decision_detail like '%Schema%' then 'EPIC Reject'
        when a.decision = 'REJECT' and a.decision_detail= 'Duplicate Customer' then 'Dedup Reject'
        when a.decision = 'REJECT' then  'Others' 
    else null
    end as Reject_Reason,
a.decision_detail, 
a.`Business Rule Reject`,
ifnull(a.uw_cost,0) uw_cost,
ifnull(a.lead_cost,0) lead_cost,
a.requestedamount as HB_requested_amount,
a.routingnumber as HB_ABA,
replace(replace(a.nmi,'$',''), ',','') as hotbox_net_monthly_income,
ifnull(a.payfrequency, '') as Pay_Frequency_HB,
a.netmonthly as HB_netmonthly,
a.city,
a.zip

from datawork.mk_application a
JOIN reporting.vmk_lead_source ls on a.lead_provider_id=ls.lead_source_id AND ls.master_source_id=25 
left join jaglms.lead_master_sources lms on ls.master_source_id = lms.master_source_id
left join reporting.leads_accepted la on a.lead_sequence_id=la.lead_sequence_id and la.isuniqueaccept=1
left join reporting.leads_accepted la2 on la.origination_loa
where (a.insert_date between '2019-11-01' and '2019-12-31')
and a.uw_cost>=0
AND a.lead_provider_id<>205  
and a.decision_detail not like 'Flow%') aa
where aa.organic_group='Affiliate');


select * from table1;

################################################NEW
/*Add
product
FPD%
Originated_loan_amount

*/

			DROP TEMPORARY TABLE IF EXISTS table1;
			CREATE TEMPORARY TABLE IF NOT EXISTS table1 ( INDEX(lead_provider_id,lead_source_id,master_source_id,lead_sequence_id) ) 
			AS (

select aa.* 
from (
select
ls.lead_source_id,
a.lead_sequence_id,
a.firstname, a.lastname, a.email, a.insert_date received_time,  
CASE WHEN length(a.state)>2 or a.state is null THEN 'X-NotAvail'
     ELSE a.state
     END AS STATE,
if(lms.master_source_id=25, 'ORG', 'EXT') AS Channel,
ls.organic_group,
ls.organic_sub_group,
ls.master_source_id, 
lms.description as lead_vendor, 
a.lead_provider_id,
ls.description as lead_campaign_name,
1 as IsLook,
if(a.uw_cost>0 or a.decision='ACCEPT', 1, 0) as IsUniqueLook,
if(a.decision='ACCEPT',1,0) as IsAccepted, 
if(n.origination_loan_id is null, 0, 1) as IsOriginated, ##

case 
    when a.decision = 'REJECT' and a.decision_detail = 'Internal Underwriting' then 'Internal_U/W'
        when a.decision = 'REJECT' and a.decision_detail like '%Schema%' then 'EPIC Reject'
        when a.decision = 'REJECT' and a.decision_detail= 'Duplicate Customer' then 'Dedup Reject'
        when a.decision = 'REJECT' then  'Others' 
    else null
    end as Reject_Reason,
a.decision_detail, 
a.`Business Rule Reject`,
ifnull(a.uw_cost,0) uw_cost,
ifnull(a.lead_cost,0) lead_cost,
a.requestedamount as HB_requested_amount,
a.routingnumber as HB_ABA,
replace(replace(a.nmi,'$',''), ',','') as hotbox_net_monthly_income,
ifnull(a.payfrequency, '') as Pay_Frequency_HB,
a.netmonthly as HB_netmonthly,
a.city,
a.zip,
n.product,
n.state as state1,
n.originated_loan_amount,
n.is_1st_payment_defaulted,
n.lms_code,
n.customer_id,
n.pay_frequency

from datawork.mk_application a
JOIN reporting.vmk_lead_source ls on a.lead_provider_id=ls.lead_source_id AND ls.master_source_id=25 
left join jaglms.lead_master_sources lms on ls.master_source_id = lms.master_source_id
left join reporting.z_AFR_Normal n on a.lead_sequence_id=n.lead_sequence_id
where (a.insert_date between '2019-01-01' and '2019-12-31')
and a.uw_cost>=0
AND a.lead_provider_id<>205  
and a.decision_detail not like 'Flow%') aa
where aa.organic_group='Affiliate');


select * from table1;