

select 
s.email,
ma.state,
ma.email as ma_email,
ma.lead_sequence_id,
max(ma.insert_date) as first_apply_date,
ma.campaign_name, ma.subid,
n.email_address,
if(sum(if(ma.decision is not null,1,0))>=1,1,0) as is_apply,
if(sum(if(ma.decision ='ACCEPT',1,0))>=1,1,0) AS is_accept,
n.is_originated,
n.application_approved_amount, n.originated_loan_amount,
if(psi.item_date is not null,1,0) as is_top_up,
sum(if(psi.item_date is not null,1,0)) as topup_cnt,
psi.item_date, 
psi.status, 
sum(psi.total_amount) as total_topup_amt
from temp.MK_Unsupported_State s
left join datawork.mk_application ma on s.email=ma.email and ma.insert_date>='2019-12-12' and ma.organization_id=2
left join reporting_cf.AFR_Normal n on s.email=n.email_address and n.original_lead_received_date>'2019-12-12'
left join jaglms.lms_payment_schedules ps on n.origination_loan_id=ps.base_loan_id
left join jaglms.lms_payment_schedule_items psi on ps.payment_schedule_id = psi.payment_schedule_id and psi.total_amount<0
                                                   and  psi.item_date>'2019-12-12'
                                                  and psi.status in ('SENT', 'Cleared')
group by ma.lead_sequence_id;




select * from  temp.MK_Unsupported_State; ##129 records

select ma.email, ma.state, ma.lead_sequence_id, ma.ACCEPT, ma.insert_date,
ma.affid, ma.decision, ma.decision_detail, ma.subid, ma.campaign_name, ma.lead_provider_id, 
ma.lead_provider_name, ma.lead_source_id, ma.lead_cost
from datawork.mk_application  ma
where ma.organization_id=2 and ma.insert_date>'2019-12-12'
and ma.email='tonya.smith31@hotmail.com';

select * from reporting_cf.leads_accepted where emailaddress='tonya.smith31@hotmail.com';

select * from reporting.leads_accepted where emailaddress='winterakawint@yahoo.com';




######################

select 
s.email,
ma.state,
ma.email as ma_email,
ma.lead_sequence_id,
ma.ACCEPT,
1 as Look,
ma.campaign_name, ma.subid,ma.uw_cost,ma.payfrequency,ma.loan_amount, ma.requestedamount,
if(ma.uw_cost>0 or ma.decision='ACCEPT', 1, 0) as IsUniqueLook,
if(ma.decision='ACCEPT',1,0) as IsAccepted,
if(n.origination_loan_id is null, 0, 1) as IsOriginated,
n.application_approved_amount, n.product,n.originated_loan_amount,
if(psi.item_date is not null,1,0) as is_top_up,
sum(if(psi.item_date is not null,1,0)) as topup_cnt,
psi.item_date, 
psi.status, 
sum(psi.total_amount) as total_topup_amt
from temp.MK_Unsupported_State s
left join datawork.mk_application ma on s.email=ma.email and ma.insert_date>='2019-12-12' and ma.organization_id=2
left join reporting_cf.AFR_Normal n on s.email=n.email_address and n.original_lead_received_date>'2019-12-12'
left join jaglms.lms_payment_schedules ps on n.origination_loan_id=ps.base_loan_id
left join jaglms.lms_payment_schedule_items psi on ps.payment_schedule_id = psi.payment_schedule_id and psi.total_amount<0
                                                   and  psi.item_date>'2019-12-12'
                                                  and psi.status in ('SENT', 'Cleared')
group by ma.lead_sequence_id;