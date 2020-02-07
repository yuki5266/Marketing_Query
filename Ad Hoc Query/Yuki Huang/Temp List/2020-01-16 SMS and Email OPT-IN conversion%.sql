SELECT n.original_lead_received_date, n.original_lead_received_month,
n.original_lead_received_day, n.original_lead_received_hour,
n.state, n.is_transactional_optin, n.is_sms_marketing_optin, n.is_originated, n.customer_id,
if(lci.optout_account_email='false' or lci.optout_account_email is NULL,1,0) as is_email_transactional_optin,
 if(lci.optout_marketing_email='false' or lci.optout_marketing_email is NULL,1,0) as  is_email_marketing_optin,
 '1' as count1, n.product, n.loan_sequence
FROM reporting.z_AFR_Normal n 
left join jaglms.lms_customer_info_flat lci on lci.customer_id=n.customer_id
where n.state='TX' and n.lms_code='JAG' and n.original_lead_received_date between '2019-09-01' and '2019-11-31' and n.lead_sequence_id>0 and n.loan_sequence=1; 


select * from jaglms.lms_customer_notifications where customer_id=392740;



