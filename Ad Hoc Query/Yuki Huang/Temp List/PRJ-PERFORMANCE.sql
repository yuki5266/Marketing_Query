select
ma.lead_sequence_id,
ma.insert_date,
ma.decision,
ma.decision_detail,
ma.campaign_name,
if(ma.decision='ACCEPT',1,0) as is_accept,
la.lms_code,
la.state,
la.product,
if(la2.origination_time>0, la2.isoriginated,la.isoriginated) as Is_originated,
if(la2.origination_time>0, la2.approved_amount,null) as Origination_loan_amount,
if(la2.origination_time>0, la2.withdrawn_reason ,la.withdrawn_reason) as withdrawn_reason,
ma.email
from  datawork.mk_application ma
-- left join jaglms.lead_master_sources ms on ms.master_source_id=ma.lead_source_id
left join jaglms.lead_source ls on ls.master_source_id=ma.lead_source_id
left join reporting.leads_accepted la on ma.lead_sequence_id = la.lead_sequence_id and la.isuniqueaccept=1
left join reporting.leads_accepted la2 on la.lms_customer_id=la2.lms_customer_id and la.lms_code=la2.lms_code and la2.origination_time>0
where ma.lead_source_id=545 and ma.insert_date>='2019-10-29';


select
ma.lead_sequence_id,
ma.insert_date,
ma.decision,
ma.decision_detail,
ma.campaign_name,
if(ma.decision='ACCEPT',1,0) as is_accept,
la.lms_code,
ifnull(la.state,ma.state) as state,
la.product,
if(la2.origination_time>0, la2.isoriginated,la.isoriginated) as Is_originated,
if(la2.origination_time>0, la2.approved_amount,null) as Origination_loan_amount,
if(la2.origination_time>0, la2.withdrawn_reason ,la.withdrawn_reason) as withdrawn_reason,
ma.email,
      afr.cnt_outbound_call_Home,
                afr.cnt_outbound_call_cell,
                afr.cnt_inbound_call_Home,
                afr.cnt_inbound_call_cell
from  datawork.mk_application ma
left join jaglms.lead_source ls on ls.master_source_id=ma.lead_source_id
left join reporting.leads_accepted la on ma.lead_sequence_id = la.lead_sequence_id and la.isuniqueaccept=1
left join reporting.leads_accepted la2 on la.lms_customer_id=la2.lms_customer_id and la.lms_code=la2.lms_code and la2.origination_time>0
left join reporting.AFR_Normal afr on ma.lead_sequence_id = afr.lead_sequence_id and afr.original_lead_received_date>='2019-10-29'
where ma.lead_source_id=545 and ma.insert_date>='2019-10-29';







select * from reporting.leads_accepted where emailaddress='alex_gambrell@aol.com';

select * from reporting.leads_accepted where la.lead_sequence_id=2306198535;

select * from datawork.mk_application ma where lead_source_id=545 and insert_date>='2019-10-29';
select * from jaglms.lead_source ;
select * from jaglms.lead_master_sources;