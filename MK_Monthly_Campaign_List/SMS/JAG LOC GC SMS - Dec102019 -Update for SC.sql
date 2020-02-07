#####################################################################
/*
JAG LOC GC condition
Active: 
For state in ('KS', 'TN', 'SC')
1. loan_status ='Originated'
2.days_since_last_draw>=15 
3.(>=40%available_credit_limit) or (<40% but >=$100)
4.<=2 defaults 
5. excluding cf.optout_marketing_email='true'
Inactive: No inactive customer will be in the list 
*/
#####################################################################
SET
@std_date= '2018-01-01',
@end_date= curdate();

DROP TEMPORARY TABLE IF EXISTS all_customer;
CREATE TEMPORARY TABLE IF NOT EXISTS all_customer ( INDEX(origination_loan_id) ) 
AS (
select 
la.lms_customer_id, 
la.lms_application_id,
la.origination_loan_id,
la.product,
la.state,
la.customer_firstname as FirstName, 
la.customer_lastname as LastName, 
la.pay_frequency,
max(la.emailaddress) as Email,
la.loan_status,
if(la.loan_status ='Originated', 'Active', if(la.loan_status ='Paid Off', 'Inactive', '')) as Status_Group,
date_format(la.origination_time,'%Y-%m-%d') as origination_date,
la.approved_amount,
(select lc.credit_limit from jaglms.loc_customer_statements lc where lc.base_loan_id=la.origination_loan_id limit 1) as original_credit_limit
from reporting.leads_accepted la 

where la.lms_code='JAG' 
and la.isoriginated=1
and la.origination_time between @std_date and @end_date
##and la.state in ('KS', 'TN', 'SC')
and la.product='LOC'
and la.loan_status ='Originated'
and la.isapplicationtest=0
group by la.lms_customer_id
);

DROP TEMPORARY TABLE IF EXISTS all_customer2;
CREATE TEMPORARY TABLE IF NOT EXISTS all_customer2 
AS (
select c.*,
       sum(if(psi.total_amount<0 and psi.status ='Cleared', psi.amount_prin, 0)) as total_draw_amount,
       sum(if(psi.total_amount<0 and psi.status ='Cleared', 1, 0)) as total_draw_count,
       sum(if(psi.total_amount>0 and psi.status in ('Cleared', 'Correction'), psi.amount_prin, 0)) as total_prin_paid,
       max(if(psi.total_amount<0 and psi.status ='Cleared', psi.item_date, null)) as last_draw_date, ##note: should we include sent?? lms_customer_id=1096150 still receive GC on 2019-12-11 even he draw money on 2019-12-10
       max(if(psi.total_amount>0 and psi.status in ('Cleared', 'Correction'), psi.item_date, null)) as last_payment_date,
       sum(if(psi.total_amount>0 and psi.status in ('Missed', 'Return'),1,0)) as total_default_count,
       sum(if(psi.total_amount>0 and psi.status in ('Cleared', 'Correction', 'Missed', 'Return'),1,0)) as total_payment_count
       
from all_customer c
left join jaglms.lms_payment_schedules ps on c.origination_loan_id=ps.base_loan_id
left join jaglms.lms_payment_schedule_items psi on ps.payment_schedule_id = psi.payment_schedule_id and psi.item_date<=curdate() 
                                                  and psi.status in ('Missed', 'Return', 'Cleared','Correction')
group by c.lms_customer_id);
                                                  
                                                  

DROP TEMPORARY TABLE IF EXISTS all_customer3;
CREATE TEMPORARY TABLE IF NOT EXISTS all_customer3 ( INDEX(lms_customer_id) ) 
AS (
select c.*,
      #(c.original_credit_limit+c.total_draw_amount+c.total_prin_paid) as available_credit_limit,
      (c.approved_amount+c.total_draw_amount+c.total_prin_paid) as available_credit_limit,
      datediff(curdate(), c.last_draw_date) as days_since_last_draw,
      datediff(curdate(), c.last_payment_date) as days_since_last_payment
from all_customer2 c); 

DROP TEMPORARY TABLE IF EXISTS exc1;
CREATE TEMPORARY TABLE IF NOT EXISTS exc1  
AS (
select distinct t1.lms_customer_id
from all_customer3 t1
join jaglms.lms_customer_info_flat cf on t1.lms_customer_id = cf.customer_id 
where cf.optout_marketing_email='true'
);

 -- select * from all_customer;
 -- select * from all_customer2;
 -- select * from all_customer3;

DROP TEMPORARY TABLE IF EXISTS all_list;
CREATE TEMPORARY TABLE IF NOT EXISTS all_list 
AS (
select f.*,
(f.available_credit_limit/f.approved_amount) as avail_credit_rate,
case when f.available_credit_limit>=f.approved_amount then '100%'
     when (f.available_credit_limit>=0.9*f.approved_amount) and (f.available_credit_limit<1*f.approved_amount)  then '90%-99%'
     when (f.available_credit_limit>=0.8*f.approved_amount) and (f.available_credit_limit<0.9*f.approved_amount)  then '80%-89%'
     when (f.available_credit_limit>=0.7*f.approved_amount) and (f.available_credit_limit<0.8*f.approved_amount)  then '70%-79%'
     when (f.available_credit_limit>=0.6*f.approved_amount) and (f.available_credit_limit<0.7*f.approved_amount)  then '60%-69%'
     when (f.available_credit_limit>=0.5*f.approved_amount) and (f.available_credit_limit<0.6*f.approved_amount)  then '50%-59%'
     when (f.available_credit_limit>=0.4*f.approved_amount) and (f.available_credit_limit<0.5*f.approved_amount)  then '40%-49%'  
     when (f.available_credit_limit>=0.3*f.approved_amount) and (f.available_credit_limit<0.4*f.approved_amount)  then '30%-39%' 
     when (f.available_credit_limit>=0.2*f.approved_amount) and (f.available_credit_limit<0.3*f.approved_amount)  then '20%-29%' 
     when (f.available_credit_limit>=0.1*f.approved_amount) and (f.available_credit_limit<0.2*f.approved_amount)  then '10%-19%' 
     else '<10%'
 end as Available_credit_range
from all_customer3 f
left join exc1 e1 on f.lms_customer_id=e1.lms_customer_id
where e1.lms_customer_id is null);

-- select * from all_list;


DROP TEMPORARY TABLE IF EXISTS all_list1;
CREATE TEMPORARY TABLE IF NOT EXISTS all_list1 
AS (
select 
      curdate() as list_generation_date,
      'JAGLOCGC_SMS' as list_module,
      date_format(now(), 'JAGLOC%m%d%YGC') as job_ID,
      al.lms_customer_id, al.lms_application_id, origination_loan_id,al.firstname, al.email, al.state, al.pay_frequency,
      al.approved_amount,al.total_draw_count, al.total_draw_amount, al.origination_date, last_payment_date,
      al.original_credit_limit,al.available_credit_limit, al.loan_status, al.Days_since_last_payment, al.avail_credit_rate,
      al.Available_credit_range, al.days_since_last_draw, al.total_default_count, al.total_payment_count
      ,case when ff.cellphone=9999999999 then ff.homephone
      when  ff.cellphone=0000000000 then ff.homephone
      when ff.cellphone=" " then  ff.homephone 
      else ff.cellphone
      end as jag_final_cellphone
      ,if(marketing.`Transactional With Consent`=1 and  marketing.`Transactional Text Stop`=0,1,0) as Is_Transactional_optin,
      if(marketing.`SMS Marketing With Consent`=1 and  marketing.`SMS Marketing Text Stop`=0,1,0) as Is_SMS_Marketing_optin
from all_list al
left join jaglms.lms_customer_info_flat ff on al.lms_customer_id = ff.customer_id 
left join
        (select list.customer_id, 
                max(list.first_consent_date_time) as first_consent_date_time,
                count(distinct if(list.notification_name = 'SMS_TRANSACTIONAL', list.customer_id, null)) as 'Is Customer for transactional',
                count(if(list.notification_name = 'SMS_TRANSACTIONAL' and list.state = 1, list.customer_id, null)) as 'Transactional With Consent',
                count(if(list.notification_name = 'SMS_TRANSACTIONAL' and (list.state=0 or list.state is null), list.customer_id, null)) as 'Transactional Without Consent',
                count(if(list.notification_name = 'SMS_TRANSACTIONAL' and list.Txt_Stop = 1, list.customer_id, null)) as 'Transactional Text Stop',     
                
                count(distinct if(list.notification_name = 'PHONE_MARKETING', list.customer_id, null)) as 'Is Customer for phone Marketing',
                count(if(list.notification_name = 'PHONE_MARKETING' and list.state = 1, list.customer_id, null)) as 'Phone Marketing With Consent',
                count(if(list.notification_name = 'PHONE_MARKETING' and (list.state=0 or list.state is null), list.customer_id, null)) as 'Phone Marketing Without Consent',
                count(if(list.notification_name = 'PHONE_MARKETING' and list.Txt_Stop = 1, list.customer_id, null)) as 'Phone Marketing Text Stop',
                
                count(distinct if(list.notification_name = 'SMS_MARKETING', list.customer_id, null)) as 'Is Customer for SMS Marketing',
                count(if(list.notification_name = 'SMS_MARKETING' and list.state = 1, list.customer_id, null)) as 'SMS Marketing With Consent',
                count(if(list.notification_name = 'SMS_MARKETING' and (list.state=0 or list.state is null), list.customer_id, null)) as 'SMS Marketing Without Consent',
                count(if(list.notification_name = 'SMS_MARKETING' and list.Txt_Stop = 1, list.customer_id, null)) as 'SMS Marketing Text Stop'
        from
              (select cn.customer_id,
                     nnm.notification_name, 
                     nnm.notification_name_mapping_id,
                     cn.first_consent_date_time,
                     cn.second_consent_date_time,
                     cn.state,
                    (select (case when s.message = 'Stop' then 1 else 0 end) from jaglms.sms_event_logs s  
                     where s.customer_id = cn.customer_id and s.notification_name_id = nnm.notification_name_mapping_id order by s.event_date desc limit 1) as 'Txt_Stop'
               from jaglms.lms_customer_notifications cn
        		   inner join jaglms.lms_notification_name_mapping nnm on cn.notification_name_id = nnm.notification_name_mapping_id) list
        group by list.customer_id) marketing  
on al.lms_customer_id=marketing.customer_id 
where (case when al.state!='SC' then days_since_last_draw>=15 and (avail_credit_rate>=0.4 or available_credit_limit>=100) and total_default_count<3
         when al.state='SC' then days_since_last_draw>=15 and available_credit_limit>=610 and total_default_count<3
         end )
and (marketing.`Transactional With Consent`=1 or marketing.`SMS Marketing With Consent`=1));

select * from all_list1;



INSERT INTO reporting.loc_gc_campaign_history
(list_generation_date, list_module, job_ID, lms_customer_id, lms_application_id, origination_loan_id,first_name,email, state, pay_frequency,
original_approved_amount,total_draw_count, total_draw_amount, origination_date, last_payment_date,
max_loan_limit,available_credit_limit, loan_status, Day_since_last_payment, avail_credit_rate,Available_credit_range, Day_since_last_topup
,total_default_count, total_payment_count,phone_number, Is_Transactional_optin,Is_SMS_Marketing_optin)
##should add phone_number, Is_Transactional_optin,Is_SMS_Marketing_optin

select 
      al.list_generation_date,
      al.list_module,
      al.job_ID,
      al.lms_customer_id, al.lms_application_id, al.origination_loan_id,al.firstname, al.email, al.state, al.pay_frequency,
      al.approved_amount,al.total_draw_count, al.total_draw_amount, al.origination_date, al.last_payment_date,
      al.original_credit_limit,al.available_credit_limit, al.loan_status, al.Days_since_last_payment, al.avail_credit_rate,
      al.Available_credit_range, days_since_last_draw, total_default_count, al.total_payment_count
      ,al.jag_final_cellphone
      ,al.Is_Transactional_optin,
      al.Is_SMS_Marketing_optin
      -- if(al.Is_Transactional_optin = 1 or Is_SMS_Marketing_optin = 1,1,0) is_sms_sent
      from all_list1 al;

-- select al.*, count(email) as cnt from all_list1 al group by email having cnt>1;


select * from reporting.loc_gc_campaign_history where list_generation_date>=curdate() and list_module='JAGLOCGC_SMS';



