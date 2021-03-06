SET 
@list_generation_time='2019-10-09',
@std_date='2019-09-10',
@end_date='2019-10-09',
@std_datetime='2019-10-10 00:00:00',
@end_datetime='2019-11-10 23:59:59';


DROP TEMPORARY TABLE IF EXISTS table1;
CREATE TEMPORARY TABLE IF NOT EXISTS table1  
AS (select
aa.*
from
(select ch.business_date, ch.list_id, ch.Channel, ch.list_name, ch.job_ID, ch.list_module, 
ch.list_frq, ch.lms_customer_id, ch.lms_application_id, ch.received_time, ch.lms_code, ch.state, ch.product, 
ch.loan_sequence, ch.email, ch.Customer_FirstName, ch.Customer_LastName,ch.list_generation_time,ch.last_repayment_date,
case
when ch.list_module = 'POL_NEW' and (case when ch.state in('CA','AL') and ch.product='SEP' then 1 else 0 end)=0 and (datediff(ch.list_generation_time, ch.last_repayment_date)) in (7,10,13,15,19,23,26,30,75) then '1' -- 2 4 5 6 7 9 10 11 22
when ch.list_module = 'POL_NEW' and (case when ch.state in('CA','AL') and ch.product='SEP' then 1 else 0 end)=0 and (datediff(ch.list_generation_time, ch.last_repayment_date)) = 60 and ch.lms_code='EPIC' then '1' -- 19
when ch.list_module = 'POL_NEW' and (case when ch.state in('CA','AL') and ch.product='SEP' then 1 else 0 end)=0 and (datediff(ch.list_generation_time, ch.last_repayment_date)) = 60 and ch.lms_code !='EPIC' then '1' -- 20
when ch.list_module = 'POL_NEW' and (case when ch.state ='CA' and ch.product='PD' then 1 else 0 end)=1 and (datediff(ch.list_generation_time, ch.last_repayment_date)) = 45 then '1' -- 14
when ch.list_module = 'POL_NEW' and (case when ch.state ='CA' and ch.product='PD' then 1 else 0 end)=1 and (datediff(ch.list_generation_time, ch.last_repayment_date)) = 60 and ch.lms_code ='EPIC'   then '1' -- 16
when ch.list_module = 'POL_NEW' and (case when ch.state ='CA' and ch.product='PD' then 1 else 0 end)=1 and (datediff(ch.list_generation_time, ch.last_repayment_date)) = 60 and ch.lms_code !='EPIC'   then '1' -- 17
when ch.list_module = 'POL_NEW' and (case when ch.state ='CA' and ch.product='SEP' then 1 else 0 end)=0 and (datediff(ch.list_generation_time, ch.last_repayment_date)) = 3  then '1' -- 1 
when ch.list_module = 'POL_NEW' and (case when ch.state in('CA','AL') and ch.product='SEP' then 1 else 0 end)=1 and (datediff(ch.list_generation_time, ch.last_repayment_date)) in (10,20,30,40,50,60,75,90) then '1' -- 3 8 12 13 15 18 21 23
else 0
end as List_filter
from reporting.campaign_history ch 
where ch.business_date between @std_date and @end_date) aa
where aa.List_filter=1 and aa.product !='LOC');


select * from table1;
select t1.*,count(distinct t1.received_time) as cnt from table1 t1 group by t1.email having cnt>1;


DROP TEMPORARY TABLE IF EXISTS table2;
CREATE TEMPORARY TABLE IF NOT EXISTS table2  
AS ( 
select distinct
t1.lms_customer_id,
t1.lms_code,
t1.state,
t1.product,
t1.loan_sequence,
t1.email,
t1.received_time as last_received_time,
max(last_repayment_date) as last_repayment_date,
max(t1.list_generation_time) as last_communication_time,
count(t1.email) as sent_attempt

from table1 t1
group by t1.lms_code,t1.lms_customer_id,t1.loan_sequence);



-- select * from table2;

DROP TEMPORARY TABLE IF EXISTS table3;
CREATE TEMPORARY TABLE IF NOT EXISTS table3  
AS (

select aa.*, 
       if(aa.Is_Internal=1, aa.IsApplied,0) as is_internal_applied,
       if(aa.Is_Internal=1, aa.IsOriginated,0) as is_internal_originated
       -- if(aa.Is_Internal=1, aa.new_loan_volume,0) as is_internal_loan_volume
       from(
select t2.*,

 1 as sent_customer,
    if(sum(if(la.lms_application_id is null,0,1))>0,1,0) IsApplied, 
    la.lms_application_id, ##
    sum(if(la.lms_application_id is null,0,1)) as Apply_cnt, 
    la.received_time as apply_time,
    la.campaign_name, 
    la.loan_sequence as loan_sequence_check,
    -- if(la.loan_sequence=t2.loan_sequence+1,1,0) as Checked,
    case when la.campaign_name like '%MK%' or la.campaign_name like 'Returning%' or la.campaign_name in ('Internal Expressed', 'Internal Expressed') then 1
         when la.lms_application_id is not null and la.campaign_name is null then 1
         when la.lms_application_id is null then ''
         else 0
         end as Is_Internal,
    la.lead_cost,
    la.pay_frequency,
    datediff(la.received_time, t2.last_repayment_date) as Days_Apply_After_PaidOff,
    datediff(max(la2.origination_time), t2.last_repayment_date) as Days_Funded_After_PaidOff,
    if(sum(if(la.origination_loan_id is null, 0,1))>0,1,0) as IsOriginated,
    max(la.origination_loan_id) as origination_loan_id, ##
    if(la.origination_loan_id is null, la.approved_amount, la2.approved_amount) as new_approved_amount,
    if(la.application_status is null,la2.application_status,la.application_status) as New_application_status,
        if(la.loan_status is null,la2.loan_status,la.loan_status) as New_loan_status
    -- if(sum(if(la.origination_loan_id is null,0,1))>0, la2.approved_amount,0) as new_loan_volume
from table2 t2
left join reporting.leads_accepted la on t2.lms_code=la.lms_code and t2.lms_customer_id = la.lms_customer_id and la.loan_sequence=t2.loan_sequence+1 and la.received_time between @std_datetime and @end_datetime and la.isuniqueaccept=1
left join reporting.leads_accepted la2 on  la.lms_code=la2.lms_code and la.lms_customer_id=la2.lms_customer_id and la.origination_loan_id=if(la.lms_code='EPIC', la2.lms_application_id, la2.loan_number)
group by t2.lms_code,t2.lms_customer_id,t2.loan_sequence) aa);



select * from table3;

select *,count(email) as cnt from table3 group by email having cnt>1; 

select * from reporting.leads_accepted where emailaddress='mkingbarnett@yahoo.com';
select * from reporting.AFR_Normal where email_address='mkingbarnett@yahoo.com';

select * from reporting.campaign_history where email='0622miranda@gmail.com' and list_module='POL_NEW' and business_date>'2019-08-29';
