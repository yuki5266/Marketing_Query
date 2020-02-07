DROP PROCEDURE IF EXISTS reporting.SP_campaign_list_gen_PA2_RC;
CREATE PROCEDURE reporting.`SP_campaign_list_gen_PA2_RC`()
BEGIN
/*********************************************************************************************************************************
---    NAME : SP_campaign_list_gen_PA2_RC
---    DESCRIPTION: script for campaign list data population
---    this initial version was created by Joyce 
---    DD/MM/YYYY    By              Comment
---    203/12/2018    Eric Pu         DAT-647 add the SP to reportingn schema with exception handling and logs
---    26/04/2019                    DAT-792 remove OH from daily/weekly SPs
---    28/06/2019       						 DAT-912 update the where condition to  if(la.loan_sequence<=7, la.loan_sequence, 7) = r.loan_sequence
																		  to handle the loan sequnce great than 7
---    26/09/2019										 DAT-983 Add approved_amount        
---    23/12/2019										 DAT-1289 Update Email Campaign SPs -to exclude CA & SC                              
************************************************************************************************************************************/
	DECLARE IsHoliday INT DEFAULT 0;
	DECLARE NotRunFlag INT DEFAULT 0;
  SET SQL_SAFE_UPDATES=0;
  SET SESSION tx_isolation='READ-COMMITTED';
  SET @start = 'Start', @end = 'End', @success = ' succeeded,', @failed = ' failed, returned SQL_STATE = ', @error_msg = ', error message = ', @total_rows = ' total row count = '; 
  SET @process_name = 'SP_campaign_list_gen_PA2_RC', @status_flag_success = 1, @status_flag_failure = 0;
  SET @valuation_date = curdate(); -- may use business date in the future
	SET @MonthNumber = Month(curdate());
  SET @DayNumber = Day(curdate());
  
  SELECT count(*) INTO IsHoliday
		FROM reporting.vw_DDR_ach_date_matching ddr
		 LEFT JOIN jaglms.business_holidays bh ON ori_date = bh.holiday
		WHERE ori_date = curdate()
			AND (ddr.weekend = 1 OR bh.description LIKE 'Thanksgiving%');
	IF (@MonthNumber = 1 AND @DayNumber = 1) OR (@MonthNumber = 12 AND @DayNumber = 25) THEN -- skip the job on Dec 25 and Jan 1
    SET NotRunFlag = 1;
	ELSEIF IsHoliday = 1 THEN  -- skip the job on weekend and US Thanksgiving Day
		SET NotRunFlag = 1;
	ELSE 
    SET NotRunFlag = 0;
  END IF;

	IF NotRunFlag = 0 THEN
		-- log the start info
		CALL reporting.SP_process_log(@valuation_date, @process_name, @start, null, 'job is running', null);
		set
		@channel='email',
		@list_name='L1 Daily Pending',
		@list_module='PA2_RC',
		@list_frq='D',
		@list_gen_time= now(),
		@time_filter='Lead_Received_Date',
		@opt_out_YN= 0,
	  @before_interval=2, 
    @before_interval2=4,
		@after_interval= -2,
    @after_interval2= -4,
    @test_job_id = 'JAG_TEST_PAJ';
    
    set
		@std_date= if( weekday(@list_gen_time) in (5,6),0,
      (if(weekday(@list_gen_time) in (2, 3, 4),(select Operation_Date from reporting.vw_DDR_ach_date_matching where Ori_date=date(Date_sub(@list_gen_time, interval @before_interval day))),
      (select Operation_Date from reporting.vw_DDR_ach_date_matching where Ori_date=date(Date_sub(@list_gen_time, interval @before_interval2 day)))
      ))), 
		@end_date= if(weekday(@list_gen_time) in (5,6),0,    
    (if(weekday(@list_gen_time) in (1, 2, 3, 4),(select date(Date_add(@list_gen_time, interval@after_interval day))),
      (select date(Date_add(@list_gen_time, interval @after_interval2 day)))  
    ))),
		@comment='Pending Application received during -2 day';

		-- select @list_name,@list_module,@list_frq,@list_gen_time,@time_filter,@opt_out_YN, @before_interval,@after_interval,@std_date,@end_date, @comment;
  
		BEGIN
    	-- Declare variables to hold diagnostics area information
    	DECLARE sql_code CHAR(5) DEFAULT '00000';
    	DECLARE sql_msg TEXT;
    	DECLARE rowCount INT;
    	DECLARE return_message TEXT;
    	-- Declare exception handler for failed insert
    	DECLARE CONTINUE HANDLER FOR SQLEXCEPTION
    		BEGIN
    			GET DIAGNOSTICS CONDITION 1
    				sql_code = RETURNED_SQLSTATE, sql_msg = MESSAGE_TEXT;
    		END;
			SET @process_label ='Main process to populate data into campaign_history', @process_type = 'Insert';
			INSERT INTO reporting.campaign_history
			(business_date, Channel,       list_name,      job_ID, list_module,    list_frq,       lms_customer_id,        lms_application_id, received_time,      lms_code,       state,  product,        loan_sequence,  email,  Customer_FirstName,
			Customer_LastName,      key_word, Req_Loan_Amount,  approved_amount,    max_loan_limit,list_generation_time,Is_Transactional_optin,Is_SMS_Marketing_optin)
			select DISTINCT
      @valuation_date,
			@channel,
			@list_name as list_name,
			case
			when la.lms_code ='TDC' then date_format(@list_gen_time, '%m%d%YPAT') 
			when la.lms_code ='JAG' then date_format(@list_gen_time, '%m%d%YPAJ')
			when la.lms_code ='EPIC' then date_format(@list_gen_time, '%m%d%YPA')
			end as job_ID, 
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
      (case when lms_code='JAG' and product = 'PD' then 'application'   -- 23/06/2017  DAT-129
            when lms_code='JAG' and product = 'SEP' then  'Installment Loan application'
            when lms_code='TDC'  then  'Line of Credit application'
            else 'application' end) as key_word, 
			ifnull(la.requested_amount, la.approved_amount) as Req_Loan_Amount, 
      la.approved_amount, -- DAT-983
			ifnull(least(r.hardcap,(ceiling(la.paycheck*r.RPP/25)*25)),1000) as Max_Loan_Limit,
			@list_gen_time as list_generation_time,
          if(marketing.`Transactional With Consent`=1 and  marketing.`Transactional Text Stop`=0,1,0) as Is_Transactional_optin,
      if(marketing.`SMS Marketing With Consent`=1 and  marketing.`SMS Marketing Text Stop`=0,1,0) as Is_SMS_Marketing_optin

			from reporting.leads_accepted la
			left join reporting.vw_loan_limit_rates r on la.state=r.state_code 
      -- and la.loan_sequence = r.loan_sequence 
      and if(la.loan_sequence<=7, la.loan_sequence, 7) = r.loan_sequence -- DAT-912
      and la.pay_frequency = r.pay_frequency 
      -- and la.product=r.product_code
      and (case when la.state='TX' and la.product='IPP' and la.storename like '%BAS%' then 'IPP-BAS'
									 when la.state='TX' and la.product='IPP' and la.storename like '%NCP%' then 'IPP-NCP'
									 else la.product end) =r.product_code  -- DAT-807
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
        group by list.customer_id) marketing  on la.lms_customer_id=marketing.customer_id 
			where la.application_status='Pending'
			and la.loan_sequence>1  and la.isreturning = 1
			and date(la.received_time) between @std_date and @end_date

            and la.state !='OH' -- DAT-792
			and IF(@opt_out_YN=1, la.Email_MarketingOptIn=1, la.Email_MarketingOptIn IN (1, 0))
			and SUBSTR(SUBSTR(la.emailaddress, INSTR(la.emailaddress, '@'), INSTR(la.emailaddress, '.')), 2) not in ('epic.lmsmail.com', 'moneykey.com')
			and SUBSTR(SUBSTR(la.emailaddress, INSTR(la.emailaddress, '@'), INSTR(la.emailaddress, '.')), 2) not like 'epic%'
      and la.IsApplicationTest = 0 -- june 19, 2017 DAT-123
      and la.state != 'MD'  -- June 2 
			and la.lms_customer_id not in
					(       select la2.lms_customer_id from reporting.leads_accepted la2
									 where la2.application_status in ('Withdrawn', 'Withdraw', 'Originated', 'Approve') and date(la2.received_time) >=date(la.received_time) )
                   and (case when la.state='CA' and la.product='SEP' then 1 else 0 end)=0;	

			-- log the process
			IF sql_code = '00000' THEN
				GET DIAGNOSTICS rowCount = ROW_COUNT;
				SET return_message = CONCAT(@process_type, @success, @total_rows,rowCount);
				CALL reporting.SP_process_log(@valuation_date, @process_name, @process_label, @process_type, return_message, @status_flag_success);
			ELSE
				SET return_message = CONCAT(@process_type, @failed, sql_code, @error_msg ,sql_msg);
				CALL reporting.SP_process_log(@valuation_date, @process_name, @process_label, @process_type, return_message, @status_flag_failure);
			END IF;
		
		END;
    -- June 20, 2017 - DAT-123 - Insert internal user info for email process verification
    BEGIN
    	DECLARE sql_code CHAR(5) DEFAULT '00000';
    	DECLARE sql_msg TEXT;
    	DECLARE rowCount INT;
    	DECLARE return_message TEXT;
    	-- Declare exception handler for failed insert
    	DECLARE CONTINUE HANDLER FOR SQLEXCEPTION
    		BEGIN
    			GET DIAGNOSTICS CONDITION 1
    				sql_code = RETURNED_SQLSTATE, sql_msg = MESSAGE_TEXT;
    		END;
			SET @process_label ='Insert internal user info for email process verification ', @process_type = 'Insert';   

      INSERT INTO reporting.campaign_history
      	(business_date, Channel, list_name, job_ID, list_module, list_frq, lms_customer_id, lms_application_id, received_time, lms_code, state, product, loan_sequence, email, Customer_FirstName, 
      	Customer_LastName, Req_Loan_Amount, origination_loan_id, origination_time,approved_amount,list_generation_time, Comments)      	 
        SELECT @valuation_date, @channel, @list_name, @test_job_id, @list_module, @list_frq, -9, -9, null, 'test', 'test', 'test', -9, 
          email_address, first_name, last_name, request_loan_amount, -9, null, approved_amount, @list_gen_time, comments
          FROM reporting.campaign_list_test_email
          WHERE is_active = 1;
   
      IF sql_code = '00000' THEN
				GET DIAGNOSTICS rowCount = ROW_COUNT;
				SET return_message = CONCAT(@process_type, @success, @total_rows,rowCount);
				CALL reporting.SP_process_log(@valuation_date, @process_name, @process_label, @process_type, return_message, @status_flag_success);
			ELSE
				SET return_message = CONCAT(@process_type, @failed, sql_code, @error_msg ,sql_msg);
				CALL reporting.SP_process_log(@valuation_date, @process_name, @process_label, @process_type, return_message, @status_flag_failure);
			END IF;
		
		END;
	 -- log the process for completion
		CALL reporting.SP_process_log(@valuation_date, @process_name, @end, null, 'job is done', @status_flag_success);
  END IF;
  
END;
