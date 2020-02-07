
/*
Steps:
1.Accepted from PRJ Campaign
2.Find the hard cap for different state
3.Find the previous approved amount
4.Calculate the conversion% and break down by month
5.Add flags
 a) If current_approved_amount > previous_approved_amount,1=true 0=False
 b) If the state_hard_Cap > previous_approved_amount, 1=true 0=False
 

*/

/*
-- Accepted# from PRJ
	DROP TEMPORARY TABLE IF EXISTS table1;
				CREATE TEMPORARY TABLE IF NOT EXISTS table1 
				AS (
        
 select * from reporting.leads_accepted 
 where campaign_name='#MK-MOB-PRJ#' 
 and date(received_time)>='2019-10-29' 
 and loan_sequence=1
 and origination_time>0);  */
 
 	DROP TEMPORARY TABLE IF EXISTS table1;
				CREATE TEMPORARY TABLE IF NOT EXISTS table1 
				AS (
        
 select lms_code, product, state, is_returning, provider_name, 
 campaign_name, dm_name, is_unique_accepts, is_express,
 lead_sequence_id, customer_id, original_lead_id, 
 loan_application_id, lms_display_number, is_originated, origination_loan_id, 
 original_lead_received_date, original_lead_received_month,
 original_lead_received_day, original_lead_received_hour,
 final_application_date, final_application_month, 
 final_application_day, is_1st_payment_defaulted, 
  application_status,current_loan_status, email_address, city, zip, originated_loan_amount, pay_frequency, sub_id
 from reporting.AFR_Normal
 where campaign_name='#MK-MOB-PRJ#' 
 and original_lead_received_date >='2019-10-29' 
 and loan_sequence=1
);
 
-- select * from table1;


-- Previous withdrawn - all
	DROP TEMPORARY TABLE IF EXISTS table2;
				CREATE TEMPORARY TABLE IF NOT EXISTS table2 (index(emailaddress,email_address))
				AS (
select 
t1.*,
la.emailaddress,
max(la.withdrawn_time) as last_withdrawn_time,
max(la.received_time) as last_received_time,
la.lms_code as withdrawn_lms_code
from reporting.leads_accepted la
inner join table1 t1 on la.emailaddress=t1.email_address -- From EPIC,TDC to JAG
where la.application_status in ('Withdrawn', 'Withdraw')
and la.loan_sequence=1
-- and date(la.withdrawn_time)<='2019-10-29'
group by la.emailaddress
);

-- select  * from table2;

           -- select * from table1 t1 where t1.email_address not in (select t2.emailaddress from table2 t2);
           
           
   	DROP TEMPORARY TABLE IF EXISTS table3;
				CREATE TEMPORARY TABLE IF NOT EXISTS table3 (index(emailaddress,last_withdrawn_time))
				AS (
         select
         la.lms_code, la.lms_customer_id, la.lms_application_id,
         la.loan_number, la.state, la.product, la.application_status, 
         la.loan_status, la.withdrawn_reason_code, la.withdrawn_time,
         la.withdrawn_reason, la.emailaddress, la.customer_firstname, 
         la.customer_lastname,
         t2.last_withdrawn_time,
         la.approved_amount as previous_approved_amount
         from table2 t2
      left join reporting.leads_accepted la on t2.emailaddress=la.emailaddress and la.withdrawn_time=t2.last_withdrawn_time
      where la.application_status in ('Withdrawn', 'Withdraw')
and la.loan_sequence=1);
           
           -- select * from table3;
           
           
           

           
           
         /*  
           -- Audit
           
           -- 513800576:1000 geral
           -- 514784455:500
           select * from reporting.leads_accepted where lms_customer_id=1216887;
           select lead_source_id
           from datawork.mk_application where lead_sequence_id=2308245335;
           
           select *,cast(aes_decrypt(ssn,SHA('109480123k0asdojf2309434joweg0oijdf0oitqqq')) as char) as SSN  from reporting.leads_accepted la where cast(aes_decrypt(ssn,SHA('109480123k0asdojf2309434joweg0oijdf0oitqqq')) as char)=513800576;
           
       */
           
           
    
           
           
           
           
           
           
           
           
           
           
           
           
           
           
           
           
           
           
           
           
   Drop TEMPORARY table if exists e1;
			Create TEMPORARY table if not exists e1 
      as(
select
cll.*,
(select value from jaglms.lms_entity_parameters lep where parameter_name = 'entity_name' and lep.lms_entity_id = cll.entity_id limit 1) as Entity_Name
from shared.credit_limit_lookup cll);





   Drop TEMPORARY table if exists e4;
			Create TEMPORARY table if not exists e4 (index (State))
      as(select e1.*,
      case when e1.Entity_Name like '%Idaho%' then 'ID'
           when e1.Entity_Name like '%Mississippi%' then 'MS'
           when e1.Entity_Name like '%Missouri%' then 'MO'
           When e1.Entity_Name like '%CBW%' then null
           else left(e1.Entity_Name,2)
           end as State from e1 e1);




  Drop TEMPORARY table if exists e2;
			Create TEMPORARY table if not exists e2 
      as(
      select e1.entity_id,e1.Entity_Name,e1.pay_frequency,max(e1.effective_date) as lastest_effective_date
      from e4 e1
      group by e1.Entity_Name,pay_frequency);
      
      
      -- select * from e2;
      
      Drop TEMPORARY table if exists e3;
			Create TEMPORARY table if not exists e3 
      as( select e1.* from e4 e1 inner join e2 e2 on e2.entity_id=e1.entity_id and e2.pay_frequency=e1.pay_frequency and e2.lastest_effective_date=e1.effective_date);
      
      -- select * from e3;
      
      Drop TEMPORARY table if exists e5;
			Create TEMPORARY table if not exists e5 
      as( select e3.effective_date,e3.Entity_name,e3.state,max(maximum_limit) as maximum_limit_byState from e3 e3 group by state);
      


-- select * from e5;

 Drop TEMPORARY table if exists table4;
			Create TEMPORARY table if not exists table4 
      as( select t1.* ,
      t3.previous_approved_amount,
      e5.maximum_limit_byState
      
      from table1 t1
      left join table3 t3 on t1.email_address=t3.emailaddress
      left join e5 e5 on e5.state=t1.state);
           
           select * from table4;
