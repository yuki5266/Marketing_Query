
select * from(
select n.is_unique_accepts, n.lms_code, n.product, n.state,n.customer_id,
 max(n.lead_received_time) as max_received_time,
max(n.original_lead_received_date) as max_received_date,
n.is_originated, max(n.origination_datetime) as max_origination_datetime, 
max(n.origination_date) as max_origination_date, n.application_status,n.current_loan_status, n.loan_application_id, n.origination_loan_id,
email_address, f.firstname,f.lastname
from reporting.z_AFR_Normal n
left join jaglms.lms_customer_info_flat f on n.customer_id=f.customer_id
where n.state in('NM','MS','MO','KS') 
and n.lms_code='JAG'
and n.application_status='Originated'
and n.current_loan_status='Originated'
group by n.customer_id) aa
where aa.max_received_date<'2020-01-22'
;








select * from jaglms.lms_customer_info_flat limit 100;

select is_originated from reporting.z_AFR_Normal where customer_id='414442';

select * from reporting.leads_accepted where lms_customer_id='934662';


