select
ma.lead_sequence_id, 
ma.insert_date,
ma.decision, 
ma.decision_detail, 
ma.campaign_name,
if(ma.decision='ACCEPT',1,0) as is_accept,
la.lms_code,la.state,la.product,
if(la2.origination_time>0, la2.isoriginated,la.isoriginated) as Is_originated,
if(la2.origination_time>0, la2.approved_amount,null) as Origination_loan_amount,
if(la2.origination_time>0, la2.withdrawn_reason ,la.withdrawn_reason) as withdrawn_reason
from  datawork.mk_application ma
left join reporting_cf.leads_accepted la on ma.lead_sequence_id = la.lead_sequence_id and la.isuniqueaccept=1
left join reporting_cf.leads_accepted la2 on la.lms_customer_id=la2.lms_customer_id and la2.origination_time>0
where ma.lead_provider_id=1673 and ma.insert_date>='2019-09-25';


select * from datawork.mk_application limit 1;
select * from reporting_cf.leads_accepted where lead_sequence_id='2298179169';

select * from reporting_cf.leads_accepted where lms_customer_id=1102028;