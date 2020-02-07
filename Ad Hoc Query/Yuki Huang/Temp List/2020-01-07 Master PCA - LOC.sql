DROP TEMPORARY TABLE IF EXISTS table1;
CREATE TEMPORARY TABLE IF NOT EXISTS table1 
AS (  
select aa.* 
from 
      (select 
      la.lms_code, 
      la.lms_customer_id,    
      la.emailaddress,
       la.customer_firstname,
       la.customer_lastname,
       la.lms_application_id, 
       la.loan_number,
       la.original_lead_id, 
      la.product, 
      la.state, 
      la.pay_frequency,
      la.loan_sequence, 
      la.isreturning, 
      la.received_time, 
      la.application_status, 
      la.loan_status, 
      la.origination_time,
      la.last_paymentdate,
      la.approved_amount,
      la.campaign_name,
      ps.is_active,
      max(psi.item_date) as last_draw_date,
      psi.item_type,
      psi.status,
      count(psi.item_date) as draw_sequence
      from reporting.leads_accepted la
      join jaglms.lms_payment_schedules ps on la.loan_number = ps.base_loan_id and ps.is_collections =0
      join jaglms.lms_payment_schedule_items psi on ps.payment_schedule_id = psi.payment_schedule_id and psi.total_amount<0 and psi.status in ('Cleared', 'SENT') 
            -- and psi.item_date <=curdate() 
      where la.isoriginated = 1
      and la.loan_status ='originated' 
      and la.product='LOC'
      and la.loan_sequence=1
      group by la.lms_customer_id) aa
where draw_sequence >=2);

-- select * from table1; -- JAG LOC Customers: draw_sequence>=2

DROP TEMPORARY TABLE IF EXISTS table2;
CREATE TEMPORARY TABLE IF NOT EXISTS table2
AS (  
select * from reporting.loc_gc_campaign_history ch
where ch.list_module LIKE '%JAGLOCGC%');


-- SELECT * FROM table2; -- LOC GC communication


DROP TEMPORARY TABLE IF EXISTS table3;
CREATE TEMPORARY TABLE IF NOT EXISTS table3
AS (  

select 
max(t2.list_generation_date) as last_communication_date,
count(*) as sent_attempt,
max(t2.total_draw_count) as last_draw_count,
t2.*
from table2 t2
group by t2.total_draw_count,t2.lms_customer_id );

 -- select * from table3;



DROP TEMPORARY TABLE IF EXISTS table4;
CREATE TEMPORARY TABLE IF NOT EXISTS table4
AS ( 

select 
t1.*,
t3.sent_attempt,
t3.last_communication_date,
t3.total_draw_count,
if(t3.last_communication_date is null,0,1) as Is_received_GC
from table1 t1
left join table3 t3 on t1.lms_customer_id=t3.lms_customer_id /*and t1.draw_sequence=t3.total_draw_count*/);

select * from table4;







select * from reporting.leads_accepted where lms_customer_id=477253 and lms_code='JAG';


select * from jaglms.lms_payment_schedule_items where payment_schedule_id in
(select payment_schedule_id from jaglms.lms_payment_schedules where base_loan_id =1089547);

select * from jaglms.lms_payment_schedules where base_loan_id =1089547

select * from reporting.loc_gc_campaign_history where email='hudsond2016@gmail.com';
