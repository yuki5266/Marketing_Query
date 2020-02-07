
##SP Validation
-- select * from reporting.campaign_history where list_module='PA_test' and business_date>'2019-12-01';

 select * from reporting.ETL_process_log where valuation_date>='2019-12-09' and process_name='SP_campaign_history_PA0_TEST_12_11';
  select * from reporting.ETL_process_log where valuation_date>='2019-12-09' and process_name='SP_campaign_history_PA0_TEST_8_12';



   ####20:00:00 to 23:59:59 
		set
		@channel='SMS',
		@list_name='L0 Daily Pending',
		@list_module='PA_test',
		@list_frq='D',
		@list_gen_time= curdate(),
		@time_filter='Lead_Received_Date',
		@opt_out_YN= 0,
		@before_interval=0, 
		@after_interval= 0,
		@test_job_id = 'JAG_TEST_PAJ',
    @valuation_date = curdate();       
        
        DROP TEMPORARY TABLE IF EXISTS table1;
CREATE TEMPORARY TABLE IF NOT EXISTS table1  
AS (  
			select DISTINCT
			@valuation_date,
			@channel,
			@list_name as list_name,
			date_format(curdate(), '%m%d%YPAJ') as job_ID,  
			@list_module as list_module,
			@list_frq as list_frq,
			la.lms_customer_id,
			la.lms_application_id,
			la.received_time,
			la.lms_code,
			la.state,
			la.product,
			la.loan_sequence,
			la.emailaddress as email,
			CONCAT(UCASE(SUBSTRING(la.customer_firstname, 1, 1)),LOWER(SUBSTRING(la.customer_firstname, 2))) as Customer_FirstName,
			CONCAT(UCASE(SUBSTRING(la.customer_lastname, 1, 1)),LOWER(SUBSTRING(la.customer_lastname, 2))) as Customer_LastName,			
			(case when lms_code='JAG' and product = 'PD' then 'application'   
            when lms_code='JAG' and product = 'SEP' then  'Installment Loan application'
            else 'application' end) as key_word, 
			ifnull(la.requested_amount, la.approved_amount) as Req_Loan_Amount, 
			ifnull(least(r.hardcap,(ceiling(la.paycheck*r.RPP/25)*25)),1000) as Max_Loan_Limit,
			@list_gen_time as list_generation_time,
      hour(la.received_time) as hour_received,
      lh.status,
      la.application_status,
      lh.created_date

from jaglms.lms_loan_header lh
left join reporting.leads_accepted la on la.lms_application_id=lh.loan_header_id and la.lms_code='JAG'
left join reporting.vw_loan_limit_rates r on la.state=r.state_code and la.loan_sequence = r.loan_sequence and la.pay_frequency = r.pay_frequency and la.product=r.product_code
where lh.status='Pending'
			and la.loan_sequence=1  
      and date(lh.created_date)=curdate()
      -- and date(lh.created_date)='2019-12-05'
			and hour(lh.created_date) between 20 and 23
			and IF(@opt_out_YN=1, la.Email_MarketingOptIn=1, la.Email_MarketingOptIn IN (1, 0))
			and SUBSTR(SUBSTR(la.emailaddress, INSTR(la.emailaddress, '@'), INSTR(la.emailaddress, '.')), 2) not in ('epic.lmsmail.com', 'moneykey.com')
			and SUBSTR(SUBSTR(la.emailaddress, INSTR(la.emailaddress, '@'), INSTR(la.emailaddress, '.')), 2) not like 'epic%'
			and la.IsApplicationTest = 0  
			and la.state != 'MD'  
            and la.state !='OH' -- DAT-792
			and la.lms_customer_id not in
					(select la2.lms_customer_id from reporting.leads_accepted la2
				     where la2.application_status in ('Withdrawn', 'Withdraw', 'Originated', 'Approve') and date(la2.received_time) >=date(lh.created_date )))
			;
			
 
  
   
   
   
    DROP TEMPORARY TABLE IF EXISTS table2;
CREATE TEMPORARY TABLE IF NOT EXISTS table2  
AS (  
   select t1.*,
   if(marketing.`Transactional With Consent`=1 and  marketing.`Transactional Text Stop`=0,1,0) as Is_Transactional_optin,
   if(marketing.`SMS Marketing With Consent`=1 and  marketing.`SMS Marketing Text Stop`=0,1,0) as Is_SMS_Marketing_optin
   from table1 t1
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
on t1.lms_customer_id=marketing.customer_id );
   
   

			INSERT INTO reporting.campaign_history

			(business_date, Channel,       list_name,      job_ID, list_module,    list_frq,       lms_customer_id,        lms_application_id, received_time,      lms_code,       state,  product,        loan_sequence,  email,  Customer_FirstName,
			Customer_LastName,      key_word, Req_Loan_Amount,      max_loan_limit,list_generation_time, is_transactional_optin, is_sms_marketing_optin )
      
      select 
        @valuation_date,
			@channel,
			@list_name as list_name,
			t2.job_ID,  
			@list_module as list_module,
			@list_frq as list_frq,
			t2.lms_customer_id,
			t2.lms_application_id,
			t2.received_time,
			t2.lms_code,
			t2.state,
			t2.product,
			t2.loan_sequence,
			t2.emailaddress as email,
			CONCAT(UCASE(SUBSTRING(t2.customer_firstname, 1, 1)),LOWER(SUBSTRING(t2.customer_firstname, 2))) as Customer_FirstName,
			CONCAT(UCASE(SUBSTRING(t2.customer_lastname, 1, 1)),LOWER(SUBSTRING(t2.customer_lastname, 2))) as Customer_LastName,			
			t2.key_word, 
			t2.Req_Loan_Amount, 
			t2.Max_Loan_Limit,
			@list_gen_time as list_generation_time,
      t2.Is_Transactional_optin,
      t2.Is_SMS_Marketing_optin
      from table2 t2;

			SET @process_label ='populate the JAG phone number into campaign_history', @process_type = 'update';
			
			update reporting.campaign_history ch
			inner join jaglms.lms_customer_info_flat c on ch.lms_customer_id= c.customer_id
            set ch.home_phone = c.homephone, ch.cell_phone = c.cellphone
			where ch.list_module = @list_module
              and ch.lms_code = 'JAG' and ch.business_date >= curdate();


######################################################################################
####00:00:00 to 11:00:00

		set
		@channel='SMS',
		@list_name='L0 Daily Pending',
		@list_module='PA_test',
		@list_frq='D',
		@list_gen_time= curdate(),
		@time_filter='Lead_Received_Date',
		@opt_out_YN= 0,
		@before_interval=0, 
		@after_interval= 0,
		@test_job_id = 'JAG_TEST_PAJ',
    @valuation_date = curdate();       
        
        DROP TEMPORARY TABLE IF EXISTS table1;
CREATE TEMPORARY TABLE IF NOT EXISTS table1  
AS (  
			select DISTINCT
			@valuation_date,
			@channel,
			@list_name as list_name,
		  date_format(curdate(), '%m%d%YPAJ') as job_ID,  
			@list_module as list_module,
			@list_frq as list_frq,
			la.lms_customer_id,
			la.lms_application_id,
			la.received_time,
			la.lms_code,
			la.state,
			la.product,
			la.loan_sequence,
			la.emailaddress as email,
			CONCAT(UCASE(SUBSTRING(la.customer_firstname, 1, 1)),LOWER(SUBSTRING(la.customer_firstname, 2))) as Customer_FirstName,
			CONCAT(UCASE(SUBSTRING(la.customer_lastname, 1, 1)),LOWER(SUBSTRING(la.customer_lastname, 2))) as Customer_LastName,			
			(case when lms_code='JAG' and product = 'PD' then 'application'   
            when lms_code='JAG' and product = 'SEP' then  'Installment Loan application'
            else 'application' end) as key_word, 
			ifnull(la.requested_amount, la.approved_amount) as Req_Loan_Amount, 
			ifnull(least(r.hardcap,(ceiling(la.paycheck*r.RPP/25)*25)),1000) as Max_Loan_Limit,
			@list_gen_time as list_generation_time,
      hour(la.received_time) as hour_received

from jaglms.lms_loan_header lh
left join reporting.leads_accepted la on la.lms_application_id=lh.loan_header_id and la.lms_code='JAG'
left join reporting.vw_loan_limit_rates r on la.state=r.state_code and la.loan_sequence = r.loan_sequence and la.pay_frequency = r.pay_frequency and la.product=r.product_code
where lh.status='Pending'
			and la.loan_sequence=1  and la.isreturning = 0
      and date(lh.created_date)=curdate()
      -- and date(lh.created_date)='2019-12-05'
			and hour(lh.created_date) between 0 and 10
			and IF(@opt_out_YN=1, la.Email_MarketingOptIn=1, la.Email_MarketingOptIn IN (1, 0))
			and SUBSTR(SUBSTR(la.emailaddress, INSTR(la.emailaddress, '@'), INSTR(la.emailaddress, '.')), 2) not in ('epic.lmsmail.com', 'moneykey.com')
			and SUBSTR(SUBSTR(la.emailaddress, INSTR(la.emailaddress, '@'), INSTR(la.emailaddress, '.')), 2) not like 'epic%'
			and la.IsApplicationTest = 0  
			and la.state != 'MD'  
            and la.state !='OH' -- DAT-792
			and la.lms_customer_id not in
					(select la2.lms_customer_id from reporting.leads_accepted la2
				     where la2.application_status in ('Withdrawn', 'Withdraw', 'Originated', 'Approve') and date(la2.received_time) >=date(lh.created_date )))
			;
			
 
  
   
   
   
    DROP TEMPORARY TABLE IF EXISTS table2;
CREATE TEMPORARY TABLE IF NOT EXISTS table2  
AS (  
   select t1.*,
   if(marketing.`Transactional With Consent`=1 and  marketing.`Transactional Text Stop`=0,1,0) as Is_Transactional_optin,
   if(marketing.`SMS Marketing With Consent`=1 and  marketing.`SMS Marketing Text Stop`=0,1,0) as Is_SMS_Marketing_optin
   from table1 t1
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
on t1.lms_customer_id=marketing.customer_id );
   
   select * from table2;

			INSERT INTO reporting.campaign_history

			(business_date, Channel,       list_name,      job_ID, list_module,    list_frq,       lms_customer_id,        lms_application_id, received_time,      lms_code,       state,  product,        loan_sequence,  email,  Customer_FirstName,
			Customer_LastName,      key_word, Req_Loan_Amount,      max_loan_limit,list_generation_time, is_transactional_optin, is_sms_marketing_optin )
      
      select 
        @valuation_date,
			@channel,
			@list_name as list_name,
			t2.job_ID,  
			@list_module as list_module,
			@list_frq as list_frq,
			t2.lms_customer_id,
			t2.lms_application_id,
			t2.received_time,
			t2.lms_code,
			t2.state,
			t2.product,
			t2.loan_sequence,
			t2.emailaddress as email,
			CONCAT(UCASE(SUBSTRING(t2.customer_firstname, 1, 1)),LOWER(SUBSTRING(t2.customer_firstname, 2))) as Customer_FirstName,
			CONCAT(UCASE(SUBSTRING(t2.customer_lastname, 1, 1)),LOWER(SUBSTRING(t2.customer_lastname, 2))) as Customer_LastName,			
			t2.key_word, 
			t2.Req_Loan_Amount, 
			t2.Max_Loan_Limit,
			@list_gen_time as list_generation_time,
      t2.Is_Transactional_optin,
      t2.Is_SMS_Marketing_optin
      from table2 t2;

			SET @process_label ='populate the JAG phone number into campaign_history', @process_type = 'update';
			
			update reporting.campaign_history ch
			inner join jaglms.lms_customer_info_flat c on ch.lms_customer_id= c.customer_id
            set ch.home_phone = c.homephone, ch.cell_phone = c.cellphone
			where ch.list_module = @list_module
              and ch.lms_code = 'JAG' and ch.business_date >= curdate();
        

 
 
 
 
 