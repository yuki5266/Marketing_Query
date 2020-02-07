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

select * from table2;

DROP TEMPORARY TABLE IF EXISTS table3;
CREATE TEMPORARY TABLE IF NOT EXISTS table3 
AS (select *,
   if(t2.`Transactional With Consent`=1 and  t2.`Transactional Text Stop`=0,1,0) as Is_Transactional_optin,
       if(t2.`SMS Marketing With Consent`=1 and  t2.`SMS Marketing Text Stop`=0,1,0) as Is_SMS_Marketing_optin


from table2 t2);
select * from table3;



DROP TEMPORARY TABLE IF EXISTS table4;
CREATE TEMPORARY TABLE IF NOT EXISTS table4 
AS (
select * from table3 t3 
inner join 

);


select * from jaglms.lms_customer_info_flat limit 1 ;