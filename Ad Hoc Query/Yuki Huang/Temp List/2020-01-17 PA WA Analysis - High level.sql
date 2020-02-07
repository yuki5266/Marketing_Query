/*
1.  NC ONLY, JAG

a. find out all the customers received from SEPT to Nov
b.Two groups
  1-Do not received any WA: it means that these customers has originated the loan only due to the PA effort
  2-Received WA: It means that these customers have received both PA and WA( Assume the campaign list works perfectly),and if this group converted, it will be attributed to the effort of WA. 



*/



	DROP TEMPORARY TABLE IF EXISTS table1;
			CREATE TEMPORARY TABLE IF NOT EXISTS table1 
			AS (

select n.loan_application_id, n.lead_id, n.lms_code, n.product, n.state, n.loan_sequence,
n.is_returning, n.provider_name, n.campaign_name, n.lead_sequence_id, 
n.WD_Reason, n.customer_id, n.original_lead_id, n.origination_loan_id, 
n.original_lead_received_date, n.original_lead_received_month,
n.original_lead_received_day, n.original_lead_received_hour, 
n.origination_datetime, n.effective_date, n.application_status, 
n.is_originated, n.day_to_origination, 
n.same_day_origination, 
n.`1_day_origination`, 
n.`2_day_origination`, 
n.`3_day_origination`,
n.`3+_day_origination`, 
-- if(timestampdiff(day, n.original_lead_received_date , date(n.origination_datetime))=4,1,0) as 4_day_origination,
n.withdrawn_reason, 
n.withdrawn_reason_detail,
n.withdrawn_datetime
from reporting.AFR_Normal n 
where n.is_returning=0 and n.original_lead_received_date between '2019-09-01' and '2019-09-31' and n.state='TX'
  );

-- SELECT * FROM  table1;

	DROP TEMPORARY TABLE IF EXISTS table2;
			CREATE TEMPORARY TABLE IF NOT EXISTS table2 
			AS (
      select
      max(ch.business_date) as last_contact_time,
      ch.lms_customer_id,
      ch.lms_application_id,
      ch.received_time,
      ch.lms_code,
      ch.email,
      max(ch.withdrawn_time) as last_withdrawn_time,
      count(*) as count1
      from reporting.campaign_history ch
      where ch.list_module='WAD' and  datediff(ch.list_generation_time, ch.withdrawn_time) in (3,10,25)
      and ch.business_date between '2019-09-01' and '2019-12-31' and ch.state='TX'
      group by ch.lms_code,ch.lms_customer_id,ch.lms_application_id );
      
      
--  select * from table2;

	DROP TEMPORARY TABLE IF EXISTS table3;
		CREATE TEMPORARY TABLE IF NOT EXISTS table3 (index(lms_application_id,loan_application_id,lms_code))
			AS (
select t1.*,
if(t2.email is null,0,1) as Is_received_WA,
t2.lms_application_id
from table1 t1
left join table2 t2 on t1.lms_code=t2.lms_code and t1.loan_application_id = t2.lms_application_id);


select * from table3;


select * from reporting.leads_accepted where loan_sequence  order by received_time desc limit 10000;

select * from reporting.leads_accepted where lms_customer_id=378518;


select * from reporting.campaign_history where email='keithwhitworth123@gmail.com';