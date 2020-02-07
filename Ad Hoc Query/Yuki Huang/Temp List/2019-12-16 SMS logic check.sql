
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
                    (select 
                    (case when s.message = 'Stop' then 1 else 0 end)
                    from jaglms.sms_event_logs s  
                     where s.customer_id = cn.customer_id and s.notification_name_id = nnm.notification_name_mapping_id order by s.event_date desc limit 1) as 'Txt_Stop'
               from jaglms.lms_customer_notifications cn
        		   inner join jaglms.lms_notification_name_mapping nnm on cn.notification_name_id = nnm.notification_name_mapping_id) list
        group by list.customer_id) marketing  
on al.lms_customer_id=marketing.customer_id 
where (case when al.state!='SC' then days_since_last_draw>=15 and (avail_credit_rate>=0.4 or available_credit_limit>=100) and total_default_count<3
         when al.state='SC' then days_since_last_draw>=15 and available_credit_limit>=610 and total_default_count<3
         end )
and (marketing.`Transactional With Consent`=1 or marketing.`SMS Marketing With Consent`=1));



###########################################

select * from jaglms.sms_event_logs s where s.message='Stop' order by s.event_date  desc limit 10000; -- Event log, transactional, record all the optin optout transactional records
select * from jaglms.lms_customer_notifications cn where cn.customer_id=382908; -- the updated records for whether or not we have consent for those three notification channel
select * from jaglms.lms_notification_name_mapping nnm limit 100;-- Mapping table for 3 notification channel, sms_transactiona, sms_markting and Phone_marketing



select * from jaglms.sms_event_logs s where s.customer_id=1129297;
select * from jaglms.lms_customer_notifications cn where cn.customer_id=1129297;
select * from jaglms.lms_customer_notifications_changelog cn where cn.customer_id=1129297;

       -- select * from jaglms.lms_customer_info_flat where homephone=cellphone limit 100;
                select * from jaglms.lms_customer_info_flat where customer_id=1129297;
                  select * from jaglms.lms_customer_info_delta where customer_id=1129297;

        
