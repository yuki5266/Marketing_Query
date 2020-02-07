set
@channel='SMS',
@list_name='Optin Good Customer',
@list_module='JAGGCSMS',
@list_frq='M',
@list_gen_time= now(),
@time_filter='Paid Off Date',
@opt_out_YN= 1,
@std_date='2015-01-01', 
@end_date=  curdate();

set @commet='JAG GC paidoff since Jan012015 with transactional optin';

/* GC List */

DROP TEMPORARY TABLE IF EXISTS jag_pay;
CREATE TEMPORARY TABLE IF NOT EXISTS jag_pay ( INDEX(lms_application_id) ) 
AS (

select distinct
@channel,
@list_name as list_name,
if(la.lms_code='EPIC',date_format(@list_gen_time, 'EPC%m%d%YGC'), date_format(@list_gen_time, 'JAG%m%d%YGC')) as job_ID,
@list_module as list_module,
@list_frq as list_frq,
la.lms_customer_id, 
la.lms_application_id,
la.received_time,
la.lms_code, 
la.state, 
la.product, 
la.storename,
la.loan_sequence, 
la.emailaddress as email,
CONCAT(UCASE(SUBSTRING(la.customer_firstname, 1, 1)),LOWER(SUBSTRING(la.customer_firstname, 2))) as Customer_FirstName,
CONCAT(UCASE(SUBSTRING(la.customer_lastname, 1, 1)),LOWER(SUBSTRING(la.customer_lastname, 2))) as Customer_LastName,
max(item_date) as last_payment_date,

@list_gen_time as list_generation_time,
@comment as comments,
la.pay_frequency,
la.approved_amount,
lbl.loan_status
from reporting.leads_accepted la
join jaglms.lms_base_loans lbl on la.loan_number=lbl.base_loan_id
join jaglms.lms_payment_schedules lps on la.lms_customer_id=lps.customer_id and la.loan_number=lps.base_loan_id 
join jaglms.lms_payment_schedule_items lpsi on lps.payment_schedule_id = lpsi.payment_schedule_id and lpsi.status='Cleared' and lpsi.total_amount>0 -- lpsi.item_type!='C'-- ='D'

where 
la.lms_code = 'JAG'
and lbl.loan_status in ('Paid Off Loan','Paid Off')
-- 'Pending Paid Off',
#'DEFAULT-PIF')
and la.state in ('DE','IL','NM','TX','UT','CA', 'AL', 'MS', 'WI', 'DE', 'ID', 'MO')
and la.isoriginated=1
and IF(@opt_out_YN=1, la.Email_MarketingOptIn=1, la.Email_MarketingOptIn IN (1, 0))
and la.IsApplicationTest=0
group by la.lms_customer_id, la.lms_application_id
-- ,lps.payment_schedule_id -- if duplicate, means in collection
     
);

-- select * from jag_pay;


DROP TEMPORARY TABLE IF EXISTS jag_gc;
CREATE TEMPORARY TABLE IF NOT EXISTS jag_gc ( INDEX(lms_application_id) ) 
AS (
select full2.*
from jag_pay full2
where Date(full2.last_payment_date) between @std_date and @end_date

-- No additional loan or Pending Application / No Bad Previous Loan (Collection) / No Withdrawal & Pending
and full2.lms_customer_id not in
    (       select la2.lms_customer_id from reporting.leads_accepted la2 
             where 
                (date(la2.origination_time) >=date(full2.last_payment_date)           -- No additional Loan
                or (la2.application_status='Pending' and date(la2.received_time) >=date(full2.last_payment_date)) -- No following Pedning Application
                or la2.loan_status in ('Returned Item','Charged Off','Default', 'DEFAULT-SLD', 'DEFAULT-BKC', 'DEFAULT-SIF','DEFAULT-FRD') -- No Previous Bad Loan
                or (la2.application_status in ('Withdrawn', 'Withdraw') and date(la2.received_time) >=date(full2.last_payment_date)
                    and la2.withdrawn_reason_code in (3,6,15,21,29))                                 -- No following Withdrawal-cannot remarket  
                 ) and la2.lms_code='JAG')
      );


DROP TEMPORARY TABLE IF EXISTS table1;
CREATE TEMPORARY TABLE IF NOT EXISTS table1 
AS (
select * from jag_gc);

-- select * from jag_pay;
-- select* from table1;

DROP TEMPORARY TABLE IF EXISTS exc;
CREATE TEMPORARY TABLE IF NOT EXISTS exc ( INDEX (email) ) 
AS (

select distinct t1.email,t1.received_time from table1 t1 
join reporting.leads_accepted t2 on t1.email=t2.emailaddress and t1.lms_code <>t2.lms_code
where t2.origination_time>=t1.last_payment_date
or (t2.application_status = 'Pending' and t2.received_time>=t1.last_payment_date)
);


-- SELECT *FROM exc;

DROP TEMPORARY TABLE IF EXISTS table2;
CREATE TEMPORARY TABLE IF NOT EXISTS table2 
AS (
select t1.*, 
       datediff(t1.list_generation_time, t1.last_payment_date) as Days_since_paid_off,
       case when datediff(t1.list_generation_time, t1.last_payment_date) <=45 then 'GC Active'
            when datediff(t1.list_generation_time, t1.last_payment_date)>45 and datediff(t1.list_generation_time, t1.last_payment_date) <=180 then 'GC Engaged'
            when datediff(t1.list_generation_time, t1.last_payment_date) >180 then 'GC Dormant'
            else null
       end as 'GC Group',
       (t1.loan_sequence+1) as Next_loan_sequence, 
       if(t1.loan_sequence+1>7, 7, t1.loan_sequence+1) as Next_loan_sequence_limit,
       case when t1.state='TX' and t1.product='IPP' and t1.storename like '%BAS%' then 'IPP-BAS'
            when t1.state='TX' and t1.product='IPP' and t1.storename like '%NCP%' then 'IPP-NCP'
            else t1.product
       end as product_limit,
       case  when t1.state='OH' AND t1.product='SP' then 'OH_SP'   #days since paid off>=12
             when t1.state='CA' AND t1.product='PD' then 'CA_PD'   ##days since paid off>=60
             when t1.state='AL' AND t1.product='SEP' then 'AL_SEP' ##days since paid off>=95
             when t1.state='CA' AND t1.product='SEP' then 'CA_SEP' ##days since paid off>=95                          
             ELSE 'OTHERS' #others days since paid off>=80
       END AS State_Filter,
              ff.homephone, 
       ff.cellphone
from table1 t1
left join jaglms.lms_customer_info_flat ff on t1.lms_customer_id = ff.customer_id and t1.lms_code='JAG'   
left join exc e on t1.email=e.email and e.received_time=t1.received_time
where e.email is null);

-- select t2.*,count(email) as cnt from table2 t2 group by t2.email having cnt>1;


DROP TEMPORARY TABLE IF EXISTS table3;
CREATE TEMPORARY TABLE IF NOT EXISTS table3 
AS (
select @Channel, t2.list_name, t2.job_ID, t2.list_module, t2.list_frq,  
       t2.lms_customer_id,  t2.lms_application_id, t2.received_time, t2.lms_code, t2.state,  t2.product,   t2.loan_sequence, t2.email,  t2.Customer_FirstName, t2.Customer_LastName,
       t2.last_payment_date, list_generation_time, @Comments, t2.pay_frequency,
       t2.approved_amount, t2.days_since_paid_off, 
       if(marketing.`Transactional With Consent`=1 and  marketing.`Transactional Text Stop`=0,1,0) as Is_Transactional_optin,
       if(marketing.`SMS Marketing With Consent`=1 and  marketing.`SMS Marketing Text Stop`=0,1,0) as Is_SMS_Marketing_optin,
      case when t2.cellphone=9999999999 then t2.homephone
           when  t2.cellphone=0000000000 then t2.homephone
           when t2.cellphone=" " then  t2.homephone 
           else t2.cellphone
      end as jag_final_cellphone,
       t2.`GC Group`,
       dd.min_amt, dd.hardcap     
from table2 t2
left join reporting.vw_loan_limit_rates dd on t2.product_limit = dd.product_code and t2.state = dd.state_code and  
                                                 if(t2.Next_loan_sequence_limit>7, 7, t2.Next_loan_sequence_limit)= dd.loan_sequence and t2.pay_frequency = dd.pay_frequency
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
on t2.lms_customer_id=marketing.customer_id and t2.lms_code='JAG'

where ((t2.State_Filter='CA_PD' and t2.Days_since_paid_off>=65) or (t2.State_Filter in ('AL_SEP') and t2.Days_since_paid_off>=95) or (t2.State_Filter='OTHERS' and t2.Days_since_paid_off>=80))
      and t2.state!='OH'
      and (marketing.`Transactional With Consent`=1 or marketing.`SMS Marketing With Consent`=1)); 



SELECT * FROM table3;











INSERT INTO reporting.monthly_campaign_history

(Channel,   list_name,  job_ID,     list_module,      list_frq,   lms_customer_id,  lms_application_id,
received_time,lms_code,   state,  product,loan_sequence, email, Customer_FirstName,      
Customer_LastName,  last_repayment_date,list_generation_time, Comments, 
pay_frequency, approved_amount, days_since_paid_off, cell_phone,
GC_Group, min_amt, hardcap,Is_Transactional_optin, Is_SMS_Marketing_optin)

select 
@Channel, t2.list_name, t2.job_ID, t2.list_module, t2.list_frq,  
       t2.lms_customer_id,  t2.lms_application_id, t2.received_time, t2.lms_code, t2.state,  t2.product,   t2.loan_sequence, 
       t2.email,  t2.Customer_FirstName, t2.Customer_LastName,
       t2.last_payment_date, list_generation_time, @Comments, t2.pay_frequency,
       t2.approved_amount, t2.days_since_paid_off, t2.jag_final_cellphone,
       t2.`GC Group`,
       t2.min_amt, t2.hardcap  ,
       t2.Is_Transactional_optin,
       t2.Is_SMS_Marketing_optin
       from table3 t2;










select list_id,Channel,   list_name,  job_ID,     list_module,      list_frq,   lms_customer_id,  lms_application_id, received_time,    lms_code,   state,      product,      loan_sequence,    email,      Customer_FirstName,      
Customer_LastName,      last_repayment_date,list_generation_time, Comments, 
is_transactional_optin, is_sms_marketing_optin, home_phone, cell_phone,
pay_frequency, approved_amount, days_since_paid_off, GC_Group, min_amt, hardcap,


from reporting.monthly_campaign_history
where list_generation_time>'10/23/2019 1:12:25 PM' and list_module='JAGGCSMS';



select *from reporting.monthly_campaign_history where date(list_generation_time)=curdate() and list_module='JAGGCSMS';

select * from reporting.monthly_campaign_history limit 100;