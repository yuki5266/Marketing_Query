select lead_id, is_returning, channel, lms_code, product, state, original_lead_id, 
customer_id, loan_application_id, lms_display_number, lead_sequence_id, loan_sequence,
WD_Reason, provider_name, campaign_name, dm_name, `is_#campaign`, 
lead_cost, application_status, current_loan_status, lead_received_time, 
original_lead_received_date, original_lead_received_month, original_lead_received_day, 
original_lead_received_hour, withdrawn_reason, withdrawn_reason_detail, withdrawn_datetime, 
minute_to_withdrawn, same_day_withdrawn, `1_day_withdrawn`, `2_day_withdrawn`, `3_day_withdrawn`,
`3+_day_withdrawn`
from reporting_cf.AFR_Normal 
where application_status in ('Withdrawn','Withdraw') 
and  original_lead_received_month='2020 January'
and loan_sequence=1 and lead_cost>1
group by customer_id;

select * from reporting_cf.leads_accepted where lms_customer_id=1129700;



select * from jaglms.lead_source where jaglms.lead_source.description like ('%ZPCF-ALL%');
select * from jaglms.lead_master_sources;
