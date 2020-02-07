-- SELECT * FROM temp.TN_SMS_EXPOSURE_LIST;

DROP TEMPORARY TABLE IF EXISTS table1;
CREATE TEMPORARY TABLE IF NOT EXISTS table1  ( index(loan_number))
AS (
SELECT tn.Loan_number,tn.Loan_Status as List_loan_status,
la.application_status,
la.loan_status,
la.lms_code,
la.lms_customer_id
FROM temp.TN_SMS_EXPOSURE_LIST tn
left join reporting.leads_accepted la on la.loan_number=tn.Loan_number where la.lms_code='JAG')
;

-- select * from table1;

DROP TEMPORARY TABLE IF EXISTS table2;
CREATE TEMPORARY TABLE IF NOT EXISTS table2 
AS (
 select list.customer_id, 
 list.Txt_Stop,
        max(list.first_consent_date_time) as first_consent_date_time,
        count(distinct if(list.notification_name = 'SMS_TRANSACTIONAL', list.customer_id, null)) as 'Is Customer for transactional',
        count(if(list.notification_name = 'SMS_TRANSACTIONAL' and list.state = 1, list.customer_id, null)) as 'Transactional With Consent',
        count(if(list.notification_name = 'SMS_TRANSACTIONAL' and (list.state=0 or list.state is null), list.customer_id, null)) as 'Transactional Without Consent',
        count(if(list.notification_name = 'SMS_TRANSACTIONAL' and list.Txt_Stop = 1, list.customer_id, null)) as 'Transactional Text Stop',     
        /*
        count(distinct if(list.notification_name = 'PHONE_MARKETING', list.customer_id, null)) as 'Is Customer for phone Marketing',
        count(if(list.notification_name = 'PHONE_MARKETING' and list.state = 1, list.customer_id, null)) as 'Phone Marketing With Consent',
        count(if(list.notification_name = 'PHONE_MARKETING' and (list.state=0 or list.state is null), list.customer_id, null)) as 'Phone Marketing Without Consent',
        count(if(list.notification_name = 'PHONE_MARKETING' and list.Txt_Stop = 1, list.customer_id, null)) as 'Phone Marketing Text Stop',
        */
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
            (select 
            (case when s.message = 'Stop' then 1 else 0 end) 
             from jaglms.sms_event_logs s  
             where s.customer_id = cn.customer_id and s.notification_name_id = nnm.notification_name_mapping_id 
             order by s.event_date desc 
             limit 1) as 'Txt_Stop'
       from jaglms.lms_customer_notifications cn
		   inner join jaglms.lms_notification_name_mapping nnm on cn.notification_name_id = nnm.notification_name_mapping_id) list
group by list.customer_id);


DROP TEMPORARY TABLE IF EXISTS table3;
CREATE TEMPORARY TABLE IF NOT EXISTS table3 
AS (select *,
   if(t2.`Transactional With Consent`=1 and  t2.`Transactional Text Stop`=0,1,0) as Is_Transactional_optin,
       if(t2.`SMS Marketing With Consent`=1 and  t2.`SMS Marketing Text Stop`=0,1,0) as Is_SMS_Marketing_optin


from table1 t1
join table2 t2 on t2.customer_id=t1.lms_customer_id);


-- select * from table3;


DROP TEMPORARY TABLE IF EXISTS table4;
CREATE TEMPORARY TABLE IF NOT EXISTS table4 
AS (select *,
if(t4.Is_SMS_Marketing_optin=0 and t4.Is_Transactional_optin=1,1,0) as Is_Need_Optout
from table3 t4);

select * from table4;
