/*
Campaign 2: OH Withdrawn, Rejects, AC (This group is included in our PRJ)
Channel: Email for all 3 audiences + SMS for Audience 1 (for Marketing opt-in)
Audience 1: Withdrawn customers with the eligible withdrawn reasons. since when? 
Audience 2: OH Rejected customers after 24 April, 2019. This list is available in Maropost. Julian will pull this list but please do a spot check.
Audience 3: OH Abandoned customers before 24 April, 2019. This list is available in Maropost. Julian will pull this list but please do a spot check.
*/

##does not include columns such as approved_amount,Req_Loan_Amount and Max_Loan_Limit from prevous application.

set
		@channel='email',
		@list_name='OH_WA_MK_to_CF',
		@list_module='WA_OH',
		@list_frq='W',
		@list_gen_time=now(),
		@time_filter='withdrawn_time',
		@opt_out_YN= 1,
    @valuation_date =curdate();

	 set
		@std_date= '2015-01-01',
		@end_date= curdate();

	
			Drop TEMPORARY table if exists raw1;
			Create TEMPORARY table if not exists raw1 as(
			select  distinct
      @valuation_date,
			@channel,
			@list_name,
			case
			when la.lms_code ='JAG' then date_format(@list_gen_time, '%m%d%YWAJ')
			else date_format(@list_gen_time, '%m%d%YWA')
			end as job_ID, 
			@list_module,
			@list_frq,
			la.lms_customer_id,
			la.lms_application_id,
			-- la.received_time,
      max(received_time) as last_received_time,
			la.lms_code,
			la.state,
			la.product,
			la.loan_sequence,
			la.emailaddress,
			CONCAT(UCASE(SUBSTRING(la.customer_firstname, 1, 1)),LOWER(SUBSTRING(la.customer_firstname, 2))) as FirstName,
			CONCAT(UCASE(SUBSTRING(la.customer_lastname, 1, 1)),LOWER(SUBSTRING(la.customer_lastname, 2))) as LastName,
			-- ifnull(la.requested_amount, la.approved_amount) as Req_Loan_Amount, 
      -- la.approved_amount, -- DAT-983
			-- la.MaxLoanLimit as Max_Loan_Limit,
			la.withdrawn_reason,
      max(la.withdrawn_time) as last_withdrawn_time, -- DAT-579
			-- if(lms_code='JAG' AND state not in ('TX', 'OH'),'Installment Loan application ',
			-- if(lms_code='TDC', 'Line of Credit application','application')) as key_word,
			@list_gen_time
			from reporting.leads_accepted la
			where
				SUBSTR(SUBSTR(emailaddress, INSTR(emailaddress, '@'), INSTR(emailaddress, '.')), 2) not in ('epic.lmsmail.com', 'moneykey.com')
			and SUBSTR(SUBSTR(emailaddress, INSTR(emailaddress, '@'), INSTR(emailaddress, '.')), 2) not like 'epic%'
			and la.IsApplicationTest = 0 
			and la.application_status in ('Withdrawn', 'Withdraw')
			and la.state ='OH'
			and withdrawn_reason_code in (1,2,10,16,19,22,23,24,25,26,27, 20)
			and la.loan_sequence=1  
      and la.isreturning = 0 
			and date(la.withdrawn_time) between @std_date and @end_date 
			and IF(@opt_out_YN=1, la.Email_MarketingOptIn=1, la.Email_MarketingOptIn IN (1, 0))
			and la.MaxLoanLimit>0
      group by lms_code, lms_customer_id);
      
      

			-- SELECT * FROM raw1;
      
      Drop TEMPORARY table if exists raw2;
			Create TEMPORARY table if not exists raw2 
      as(
      select * from raw1 r1 
      where r1.lms_customer_id not in (select la2.lms_customer_id from reporting.leads_accepted la2 where (la2.application_status='pending' or la2.origination_time is not null)) );
      
      -- select * from raw2;
    
			      Drop TEMPORARY table if exists raw3;
			Create TEMPORARY table if not exists raw3 
      as(
      select * from raw2 r2 
      where r2.emailaddress not in (select distinct lacf.emailaddress from reporting_cf.leads_accepted lacf where lacf.emailaddress is not null));
      
      select * from raw3;
      
      Drop TEMPORARY table if exists table2;
			Create TEMPORARY table if not exists table2 as(
      select 
      r3.*,
      IF(r3.lms_code = 'JAG',
       (CASE
           WHEN ff.cellphone = 9999999999 THEN ff.homephone
           WHEN ff.cellphone = 0000000000 THEN ff.homephone
           WHEN ff.cellphone = " " THEN ff.homephone
           ELSE ff.cellphone
        END),
       (CASE
           WHEN tt.cellphone = '(999)999-9999' THEN tt.homephone
           WHEN tt.cellphone = " " THEN tt.homephone
           ELSE tt.cellphone
        END)) as cell_phone
      from raw3 r3
      left JOIN jaglms.lms_customer_info_flat ff ON r3.lms_customer_id = ff.customer_id AND r3.lms_code = 'JAG'
      left JOIN ais.vw_client tt ON r3.lms_customer_id = tt.id AND r3.lms_code = 'EPIC');
      
      
      -- select * from table2;

    
    
   

			-- INSERT INTO reporting.campaign_history

			(business_date, Channel,       list_name,      job_ID, list_module,    list_frq,       lms_customer_id,        lms_application_id, received_time,      lms_code,       state,  product,        loan_sequence,  email, 
      Customer_FirstName,
			Customer_LastName,      Req_Loan_Amount,approved_amount, Max_Loan_Limit,withdrawn_reason, withdrawn_time,list_generation_time, Is_Transactional_optin,Is_SMS_Marketing_optin)


			select 
      @valuation_date,
      @Channel,
      @list_name,
      t2.Job_ID,
      @list_module,
      @list_frq,
      t2.lms_customer_id,
      t2.lms_application_id,
      t2.last_received_time,
      t2.lms_code,
      t2.state,
      t2.product,
      t2.loan_sequence,
      t2.emailaddress,
      t2.FirstName,
      t2.LastName,
      -- t2.Req_Loan_Amount,
      -- t2.approved_amount, 
      -- t2.Max_Loan_Limit,
      t2.withdrawn_reason, 
      t2.last_withdrawn_time,
      @list_gen_time,
      1 as Is_Transactional_optin,
       case
         when t2.lms_code='JAG' then if(marketing.`SMS Marketing With Consent`=1 and  marketing.`SMS Marketing Text Stop`=0,1,0)
         when t2.lms_code='EPIC' then 0
         else null
         end as Is_SMS_Marketing_optin
      from table2 t2
      left join jaglms.lms_customer_info_flat ff on t2.lms_customer_id = ff.customer_id and t2.lms_code='JAG'   
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
on t2.lms_customer_id=marketing.customer_id;

      
    
