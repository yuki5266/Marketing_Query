## Checked if the customer come back and proceed with application since last_contact time within 7 days period. 
## ( received_time between last_contact_time and last_contact_time +7 days) 
## KPI: applied%,conversion% and survival% 
-- applied% = applied/sent
-- conversion% = Originated/accepted
-- survival% = Originated/sent

## Logic for AC updated on 2019-08-12 to exclude 'Rejected' customer receiving the AC email. Check the date before to see if we sent the AC to the wrong customer
## If the customer hit the 'submit_payment_schedule' page and submit--> should have lead_sequence_id since the lead will go through HB
## all customer should be NC and loan_sequence =1
## reveived_time = drop_off_time (the time when customer left the application process)

/*
Question:
1. AC query does not check If the HB has already accepte this customer? Example: Betty.tyson14@gmail.com, hrwoodland@icloud.com
  this customer has been accepted by us on 10/3/2019 5:03:46 PM, but we still send her AC on 10/4/2019 10:20:57 AM.
  Not sure if the webapi.tracking table update by lead_sequence_id when the customer accepted by HB.
2. There are some customers have been tracked the wrong last_drop_off_time so they still got AC even though they have submited the payment schedule.
  Suspicious are there might be delay on the updating lead_sequence_id on the webapi.tracking table.(AC: if lead_sequence_id is null, the customer should receive AC)
  Example:Umana4@live.com,riza_gamboa@yahoo.com
3. riza_gamboa@yahoo.com: reject,pre-approved and the reject again???



*/

select * from webapi.tracking where user_name='Mona44123@yahoo.com';
select * from reporting_cf.campaign_history where email ='Betty.tyson14@gmail.com';
select emailaddress, received_time, lms_customer_id, application_status, IsExternal, master_source_id, provider_name from reporting_cf.leads_accepted where emailaddress='hrwoodland@icloud.com';
select * from datawork.mk_application where email ='hrbond34@att.net';
select email from datawork.mk_application where lead_sequence_id=2287863406;
select count(*) from reporting_cf.campaign_history where list_module in('ACH','ACB','ACD') group by email;


##Retrive all the campaign history
DROP TEMPORARY TABLE IF EXISTS table1;
CREATE TEMPORARY TABLE IF NOT EXISTS table1  
AS (
select    
    ch.email,
    min(ch.received_time) as first_drop_off_time,
    min(ch.list_generation_time) as first_communication_time,
    max(ch.received_time) as last_drop_off_date,
    max(ch.list_generation_time) as last_communication_time,
    count(distinct ch.list_generation_time) as Sent_Attempt,
       max(ch.list_generation_time) as evaluation_start_date, 
    date_sub(max(ch.list_generation_time),interval -7 day) as evaluation_end_date,
    case
when la.received_time between min(ch.received_time) and min(ch.list_generation_time) and la.master_source_id=93 then 11  -- exclude any customer shouldn't receive AC in the first time since they still continue application
when (la.received_time between max(ch.received_time) and max(ch.list_generation_time)) and la.master_source_id=93 and count(distinct ch.list_generation_time)>1 then 11
when la.received_time<min(ch.received_time) then 0 -- exclude the customer that has been accepted by HB but still has activity on our website. Should exclude these customers in the future?
when la.master_source_id !=93 then 0 -- only evaluate the CF Internal
else 1
end as Is_correct_sent,

    case
when la.received_time between min(ch.received_time) and min(ch.list_generation_time) and la.master_source_id=93 then 'Accepted before AC_Internal'  -- exclude any customer shouldn't receive AC in the first time since they still continue application
when (la.received_time between max(ch.received_time) and max(ch.list_generation_time)) and la.master_source_id=93 and count(distinct ch.list_generation_time)>1 then 'Accepted after AC_Internal'
when la.received_time<min(ch.received_time) and la.master_source_id !=93 then 'Accepted before AC_External' -- exclude the customer that has been accepted by HB but still has activity on our website. Should exclude these customers in the future?
when la.master_source_id !=93 then 'Accepted before AC_External' -- only evaluate the CF Internal
else 'Internal_AC'
end as Group1,

if((sum(if(wt.page='reject',1,0))>=1) and (sum(if(ch.list_generation_time<'2019-08-12',1,0))>=1),1,0) as is_before_change,-- check how  many customer receive AC even though they got rejected on Website
la.application_status,
la.loan_status,
la.received_time,
la.origination_time,
la.origination_loan_id,
la.provider_name,
la.isoriginated,
la.state,
la.product, la.withdrawn_reason
from reporting_cf.campaign_history ch
left join reporting_cf.leads_accepted la on ch.email=la.emailaddress and la.lms_code='JAG' and la.isuniqueaccept=1 -- and la.master_source_id=93 -- LOC only has one record on the la table
left join webapi.tracking wt on wt.user_name=ch.email and wt.page='reject' 
where ch.list_module  in ('ACH', 'ACD', 'ACB')
group by ch.email
having last_communication_time<date_sub(curdate(),interval 7 day));


-- select * from table1;


##check if the customer come back and submit the application

DROP TEMPORARY TABLE IF EXISTS table2;
CREATE TEMPORARY TABLE IF NOT EXISTS table2  
AS (
Select 
t1.*,
if(t1.Is_correct_sent = 1 and count(wt1.page)>=1 and (min(wt1.`timestamp`) between t1.evaluation_start_date and evaluation_end_date),1,if(t1.Is_correct_sent = 11,1,0)) as Is_come_back_7days,
if(t1.Is_correct_sent = 1 and (min(wt1.`timestamp`)>=t1.evaluation_start_date),min(wt1.`timestamp`),null) as comeback_time_7days,
if(t1.Is_correct_sent = 1 and (sum(if(wt1.page='payment_schedule_submit',1,0))>=1),1,if(t1.Is_correct_sent=11 and t1.received_time>0,1,0)) as is_submit_7days,
if(t1.Is_correct_sent = 1 and (t1.received_time between t1.evaluation_start_date and t1.evaluation_end_date) and (sum(if(wt1.page='payment_schedule_submit',1,0))>=1),1,if(t1.Is_correct_sent=11 and t1.received_time>0,1,0)) as Is_accepted_7days,
if(t1.Is_correct_sent = 1 and (t1.received_time between t1.evaluation_start_date and t1.evaluation_end_date),t1.isoriginated,if(t1.Is_correct_sent=11,t1.isoriginated,0)) as Is_Originated_7days,
if(t1.Group1 in ('Internal_AC','Accepted before AC_Internal' ,'Accepted after AC_Internal'),1,0) as sent_customer
-- if(la.received_time<t1.first_drop_off_time,1,0) as is_before_first_contact
from table1 t1
left join webapi.tracking wt1 on t1.email= wt1.user_name and wt1.`timestamp` between t1.evaluation_start_date and t1.evaluation_end_date

group by t1.email);


-- select * from table2;


##Check draw count
DROP TEMPORARY TABLE IF EXISTS table3;
CREATE TEMPORARY TABLE IF NOT EXISTS table3  
AS (select 
t2.*,
ps.is_active,
max(psi.item_date) as last_draw_date,
-- psi.item_type,
-- psi.status,
count(psi.item_date) as draw_sequence
from table2 t2 left join jaglms.lms_payment_schedules ps on t2.origination_loan_id = ps.base_loan_id and ps.is_collections =0
             left join jaglms.lms_payment_schedule_items psi on ps.payment_schedule_id = psi.payment_schedule_id and psi.total_amount<0 and psi.status in ('Cleared', 'SENT') 
            and psi.item_date <=curdate()
            -- where t2.is_correct_sent=1   
            group by t2.email
         );
            
select * from table3;