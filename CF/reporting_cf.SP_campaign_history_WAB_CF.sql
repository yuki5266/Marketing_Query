DROP PROCEDURE IF EXISTS reporting_cf.SP_campaign_history_WAB_CF;
CREATE PROCEDURE reporting_cf.`SP_campaign_history_WAB_CF`()
BEGIN
/*******************************************************************************************************************
---    NAME : SP_campaign_history_WAB_CF
---    DESCRIPTION: script for populate table  campaign_history for list model WAB in reporting_cf schema
---    DD/MM/YYYY    By              Comment
---    22/07/2019    Eric Pu         DAT-948 wrape it based on Joyce's script and add exception handling and logs
																		 The running schedule is 10:30 am EST every 1st & 3rd Wednesday of the Month.

*********************************************************************************************************************/
  SET SQL_SAFE_UPDATES=0;
  SET @start = 'Start', @end = 'End', @success = ' succeeded,', @failed = ' failed, returned SQL_STATE = ', @error_msg = ', error message = ', @total_rows = ' total row count = '; 
  SET @process_name = 'SP_campaign_history_WAB_CF', @status_flag_success = 1, @status_flag_failure = 0;
  SET @valuation_date = curdate();  
  SELECT HOUR(CURTIME()) INTO @runhour;
  SELECT weekday(curdate()) INTO @weekday; 
  SELECT FLOOR((DAYOFMONTH(curdate()) - 1)/7 +1) INTO @weeknum; 

  IF @weekday = 2 and @weeknum in (1,3) THEN -- only run every 1st & 3rd Wednesday of the Month 
	 
		-- log the start info
		CALL reporting.SP_process_log(@valuation_date, @process_name, @start, null, 'job is running', null);
    set
		@channel='email',
		@list_name='L1 Daily Withdrawn',
		@list_module='WAB',
		@list_frq='B',
		@list_gen_time=now(),
		@time_filter='withdrawn_time',
		@opt_out_YN= 1,
		@std_week_interval= 1, 
		@end_week_interval= 0,
    @test_job_id = 'JAG_TEST_WAJ'; 

		set
    @first_interval= 30,
		@comment='Withdrawn 30 days ago';
  
  	-- select @valuation_date, @channel,@list_name,@list_module,@list_frq,@list_gen_time,@time_filter,@opt_out_YN, @first_interval,@second_interval,@third_interval,@first_date,@second_date,@third_date, @comment;
       
	
		BEGIN
			-- Declare variables to hold diagnostics area information
			DECLARE sql_code CHAR(5) DEFAULT '00000';
			DECLARE sql_msg TEXT;
			DECLARE rowCount INT;
			DECLARE return_message TEXT;
			DECLARE CONTINUE HANDLER FOR SQLEXCEPTION
			BEGIN
				GET DIAGNOSTICS CONDITION 1
					sql_code = RETURNED_SQLSTATE, sql_msg = MESSAGE_TEXT;
			END;
			SET @process_label ='prepare temporary tables', @process_type = 'Create';
			 
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
			la.received_time,
			la.lms_code,
			la.state,
			la.product,
			la.loan_sequence,
			la.emailaddress,
			CONCAT(UCASE(SUBSTRING(la.customer_firstname, 1, 1)),LOWER(SUBSTRING(la.customer_firstname, 2))) as FirstName,
			CONCAT(UCASE(SUBSTRING(la.customer_lastname, 1, 1)),LOWER(SUBSTRING(la.customer_lastname, 2))) as LastName,
			ifnull(la.requested_amount, la.approved_amount) as Req_Loan_Amount, 
      la.approved_amount,
			la.MaxLoanLimit as Max_Loan_Limit,
			la.withdrawn_reason,
      la.withdrawn_time, 
			null as key_word,

			@list_gen_time,

			if(la.campaign_name like '%DM%',1,0) as IsDM

			from reporting_cf.leads_accepted la
			where
				SUBSTR(SUBSTR(emailaddress, INSTR(emailaddress, '@'), INSTR(emailaddress, '.')), 2) not in ('epic.lmsmail.com', 'moneykey.com')
			and SUBSTR(SUBSTR(emailaddress, INSTR(emailaddress, '@'), INSTR(emailaddress, '.')), 2) not like 'epic%'
			and la.IsApplicationTest = 0 
			and la.application_status in ('Withdrawn', 'Withdraw')
			
			and withdrawn_reason_code in (1,2,10,16,19,22,23,24,25,26,27, 20)
			and la.loan_sequence=1  and la.isreturning = 0 
			and date(la.withdrawn_time)<= date_sub(curdate(), interval 30 day)
           
			and IF(@opt_out_YN=1, la.Email_MarketingOptIn=1, la.Email_MarketingOptIn IN (1, 0))
			and la.MaxLoanLimit>0
			and la.lms_customer_id not in
					(select la2.lms_customer_id
						from reporting_cf.leads_accepted la2 where (la2.application_status='pending' or la2.origination_time is not null) )
						);


			Drop TEMPORARY table if exists raw2;
			Create TEMPORARY table if not exists raw2 as(
			select  
			lms_customer_id,
			max(received_time) as last_received_time,
			lms_code,
			emailaddress
			from raw1
			group by lms_code, lms_customer_id);
	 
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

		BEGIN
			DECLARE sql_code CHAR(5) DEFAULT '00000';
			DECLARE sql_msg TEXT;
			DECLARE rowCount INT;
			DECLARE return_message TEXT;

			DECLARE CONTINUE HANDLER FOR SQLEXCEPTION
			BEGIN
				GET DIAGNOSTICS CONDITION 1
					sql_code = RETURNED_SQLSTATE, sql_msg = MESSAGE_TEXT;
			END;
			SET @process_label ='populate data for list module WAB', @process_type = 'Insert';
	 
			INSERT INTO reporting_cf.campaign_history
			(business_date, Channel, list_name,job_ID, list_module,list_frq, lms_customer_id,lms_application_id, received_time,lms_code,state,product,loan_sequence,email,Customer_FirstName,
			Customer_LastName,Req_Loan_Amount, approved_amount, Max_Loan_Limit,withdrawn_reason, withdrawn_time, key_word,list_generation_time, extra1)
    	select raw1.* from raw1 inner join raw2 on raw1.lms_code=raw2.lms_code and raw1.lms_customer_id=raw2.lms_customer_id and raw1.received_time=raw2.last_received_time;
	
      -- insert test data
      INSERT INTO reporting_cf.campaign_history
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
