DROP PROCEDURE IF EXISTS reporting.SP_campaign_list_gen_D_WA;
CREATE PROCEDURE reporting.`SP_campaign_list_gen_D_WA`()
BEGIN
/***************************************************************************************************************************************
---    NAME : SP_campaign_list_gen_D_WA
---    DESCRIPTION: script for campaign list data population
---    this initial version was created by Joyce in temp schema, 
---    DD/MM/YYYY    By              Comment
---    05/12/2018    Eric Pu         DAT-647 the logic same as SP_campaign_list_gen_W_WA but schedule to run daily
---    26/04/2019                    DAT-792 remove OH from daily/weekly SPs
---    14/08/2019										 DAT-983 Add approved_amount
---    07/11/2019										 DAT-1172 extend the start date from 30 days ago to 90
---    20/12/2019                    DAT-1288 update reporting.SP_campaign_list_gen_D_WA to exclude SC LOC on the WA list.
****************************************************************************************************************************************/
	DECLARE IsHoliday INT DEFAULT 0;
	DECLARE NotRunFlag INT DEFAULT 0;
  SET SQL_SAFE_UPDATES=0;
  SET @start = 'Start', @end = 'End', @success = ' succeeded,', @failed = ' failed, returned SQL_STATE = ', @error_msg = ', error message = ', @total_rows = ' total row count = '; 
  SET @process_name = 'SP_campaign_list_gen_D_WA', @status_flag_success = 1, @status_flag_failure = 0;
  SET @valuation_date = curdate(); -- may use business date in the future
  SET @MonthNumber = Month(curdate());
  SET @WeekDayNumber = Weekday(curdate());
  SET @DayNumber = Day(curdate());
  
 SELECT count(*) INTO IsHoliday
		FROM reporting.vw_DDR_ach_date_matching ddr
		 LEFT JOIN jaglms.business_holidays bh ON ori_date = bh.holiday
		WHERE ori_date = curdate()
			AND (ddr.weekend = 1 OR bh.description LIKE 'Thanksgiving%');
	IF (@MonthNumber = 1 AND @DayNumber = 1) OR (@MonthNumber = 12 AND @DayNumber = 25) OR IsHoliday = 1 THEN -- skip the job on Dec 25, Jan 1, and US Thanksgiving Day
    SET NotRunFlag = 1;
-- 	ELSEIF @WeekDayNumber <> 1 THEN  -- only run on Tuesday
	-- 	SET NotRunFlag = 1;
	ELSE 
    SET NotRunFlag = 0;
  END IF;

	IF NotRunFlag = 0 THEN
		-- log the start info
		CALL reporting.SP_process_log(@valuation_date, @process_name, @start, null, 'job is running', null);
		
		set
		@channel='email',
		@list_name='L1 Daily Withdrawn',
		@list_module='WAD',
		@list_frq='D',
		@list_gen_time=now(),
		@time_filter='withdrawn_time',
		@opt_out_YN= 1,
		@std_week_interval= 1, 
		@end_week_interval= 0,
    @test_job_id = 'JAG_TEST_WAJ'; 

		set
		@std_date= if(weekday(@list_gen_time) in (5,6),0,
		(select date(Date_sub(@list_gen_time, interval 90 day)))), -- DAT-1172
		@end_date= if(weekday(@list_gen_time) in (5,6),0,
		 (select date(Date_sub(@list_gen_time, interval 1 day)))),
		@comment='Withdrawn from 30 days ago to yesterday';
		select @channel, @list_name,@list_module,@list_frq,@list_gen_time,@time_filter,@opt_out_YN, @std_week_interval,@end_week_interval,@std_date,@end_date, @comment;

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
			SET @process_label ='Create temporary table raw1', @process_type = 'Create';
	
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
      la.approved_amount, -- DAT-983
			la.MaxLoanLimit as Max_Loan_Limit,
			la.withdrawn_reason,
      la.withdrawn_time, -- DAT-579
			if(lms_code='JAG' AND state not in ('TX', 'OH'),'Installment Loan application ',
			if(lms_code='TDC', 'Line of Credit application','application')) as key_word,

			@list_gen_time,

			-- if(la.campaign_name in ('MK-WEB-DM', 'MK-PHONE-DM', 'MK-MOB-DM'),1,0) as IsDM
      if(la.campaign_name like '%DM%',1,0) as IsDM -- DAT-983
			from reporting.leads_accepted la
			where
				SUBSTR(SUBSTR(emailaddress, INSTR(emailaddress, '@'), INSTR(emailaddress, '.')), 2) not in ('epic.lmsmail.com', 'moneykey.com')
			and SUBSTR(SUBSTR(emailaddress, INSTR(emailaddress, '@'), INSTR(emailaddress, '.')), 2) not like 'epic%'
			and la.IsApplicationTest = 0 -- june 19, 2017 DAT-123
			and la.application_status in ('Withdrawn', 'Withdraw')
			-- and la.state in ('DE','IL', 'KS','MO','NM','OH','TX','UT', 'SC', 'CA','AL', 'MS')
			and withdrawn_reason_code in (1,2,10,16,19,22,23,24,25,26,27, 20)
			and la.loan_sequence=1  and la.isreturning = 0 
			and date(la.withdrawn_time) between @std_date and @end_date -- DAT253 change to use withdrawn_time insteand of receive_time
           -- and la.state !='OH' -- DAT-792
			and la.state not in ('OH','SC', 'CA') -- DAT-1288 / DAT-1289
			and IF(@opt_out_YN=1, la.Email_MarketingOptIn=1, la.Email_MarketingOptIn IN (1, 0))
			-- and la.MaxLoanLimit>0 DAT-1287
			and la.lms_customer_id not in
					(select la2.lms_customer_id
						from reporting.leads_accepted la2 where (la2.application_status='pending' or la2.origination_time is not null) )
						);
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
    	-- Declare exception handler for failed insert
    	DECLARE CONTINUE HANDLER FOR SQLEXCEPTION
    		BEGIN
    			GET DIAGNOSTICS CONDITION 1
    				sql_code = RETURNED_SQLSTATE, sql_msg = MESSAGE_TEXT;
    		END;
		  SET @process_label ='Create temporary table raw2', @process_type = 'Create';

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
    	-- Declare exception handler for failed insert
    	DECLARE CONTINUE HANDLER FOR SQLEXCEPTION
    		BEGIN
    			GET DIAGNOSTICS CONDITION 1
    				sql_code = RETURNED_SQLSTATE, sql_msg = MESSAGE_TEXT;
    		END;
			SET @process_label ='Populate data into campaign_history', @process_type = 'Insert';

			INSERT INTO reporting.campaign_history

			(business_date, Channel,       list_name,      job_ID, list_module,    list_frq,       lms_customer_id,        lms_application_id, received_time,      lms_code,       state,  product,        loan_sequence,  email,  Customer_FirstName,
			Customer_LastName,      Req_Loan_Amount,approved_amount, Max_Loan_Limit,withdrawn_reason, withdrawn_time, key_word,list_generation_time, extra1)


			select raw1.* from raw1 inner join raw2 on raw1.lms_code=raw2.lms_code and raw1.lms_customer_id=raw2.lms_customer_id and raw1.received_time=raw2.last_received_time;

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
    -- June 19, 2017 - DAT-123 - Insert internal user info for email process verification
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
