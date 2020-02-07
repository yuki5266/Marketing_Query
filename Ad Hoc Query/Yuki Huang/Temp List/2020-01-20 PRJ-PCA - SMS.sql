select * from reporting.z_AFR_Normal where sub_id='prj-002' and date(lead_received_time)  >'2019-10-29';

select * from reporting.leads_accepted where lead_sequence_id=2218479683;

select * from reporting.AFR_Normal where lead_sequence_id=2218479683;

select * from reporting.vmk_lead_source;



select * from reporting.leads_accepted where lead_source_id in(545,546) and subid='prj-002';

select * from reporting.leads_accepted where lms_code='JAG' and lms_customer_id=725213;

select
a.state,
a.lead_sequence_id,
a.subid,
a.insert_date,
1 as IsLook,
if(a.uw_cost>0 or a.decision='ACCEPT', 1, 0) as IsUniqueLook,
if(a.decision='ACCEPT',1,0) as IsAccepted, 
if(la.origination_loan_id is null, 0, 1) as IsOriginated,
la.product,
la.pay_frequency,
n.requested_amount
from datawork.mk_application a
-- JOIN reporting.vmk_lead_source ls on a.lead_provider_id=ls.lead_source_id AND ls.master_source_id=25 
-- left join jaglms.lead_master_sources lms on ls.master_source_id = lms.master_source_id
left join reporting.leads_accepted la on a.lead_sequence_id=la.lead_sequence_id and la.origination_loan_id is not null
left join reporting.z_AFR_Normal n on la.lead_sequence_id = n.lead_sequence_id and n.origination_loan_id is not null
where a.subid='prj-002' and a.insert_date>'2019-10-29';