DROP PROCEDURE IF EXISTS reporting_cf.SP_campaign_list_gen_PA_CF;
CREATE PROCEDURE reporting_cf.`SP_campaign_list_gen_PA_CF`()
BEGIN
/*********************************************************************************************************************************
---    NAME : reporting_cf.SP_campaign_list_gen_PA_CF
---    DESCRIPTION: script for campaign list data population
---    this initial version was created by Joyce in temp schema, 
---    DD/MM/YYYY    By              Comment
---    29/04/2019    Joyce/Eric      initial version
---    09/05/2019                    DAT-807 update reporting SP: PA due to IPP changed to IPP-BAS and IPP-NCP
---    28/06/2019                    DAT-914 Add approved_amount in reporting_cf.SP (PA and WAD)
************************************************************************************************************************************/
  DECLARE IsHoliday INT DEFAULT 0;
  DECLARE NotRunFlag INT DEFAULT 0;
  SET SQL_SAFE_UPDATES=0;
  SET SESSION tx_isolation='READ-COMMITTED';
  SET @start = 'Start', @end = 'End', @success = ' succeeded,', @failed = ' failed, returned SQL_STATE = ', @error_msg = ', error message = ', @total_rows = ' total row count = '; 
  SET @process_name = 'SP_campaign_list_gen_PA_CF', @status_flag_success = 1, @status_flag_failure = 0;
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
		@list_module='PA',
		@list_frq='D',
		@list_gen_time= now(),
		@time_filter='Lead_Received_Date',
		@opt_out_YN= 0,
		@before_interval=1, 
		@after_interval= -1,
    @test_job_id = 'JAG_TEST_PAJ';
    
		set
		@std_date= if(weekday(@list_gen_time) in (5,6),0,(select Operation_Date from reporting.vw_DDR_ach_date_matching where Ori_date=date(Date_sub(@list_gen_time, interval @before_interval day)))),
		@end_date= if(weekday(@list_gen_time) in (5,6),0,date(Date_add(@list_gen_time, interval @after_interval day))),
		@comment='Pending Application received during -1 day';
		select @list_name,@list_module,@list_frq,@list_gen_time,@time_filter,@opt_out_YN, @before_interval,@after_interval,@std_date,@end_date, @comment;

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
			
			INSERT INTO reporting_cf.campaign_history

			(business_date, Channel,       list_name,      job_ID, list_module,    list_frq,       lms_customer_id,        lms_application_id, received_time,      lms_code,       state,  product,        loan_sequence,  email,  Customer_FirstName,
			Customer_LastName,      key_word, Req_Loan_Amount, approved_amount,   max_loan_limit,list_generation_time)

			select DISTINCT
      @valuation_date,
			@channel,
			@list_name as list_name,
			case
			when la.lms_code ='JAG' then date_format(@list_gen_time, '%m%d%YPAJ')
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
      'Line of Credit application' as key_word, 
			ifnull(la.requested_amount, la.approved_amount) as Req_Loan_Amount, 
      la.approved_amount, -- DAT-914
			ifnull(least(r.hardcap,(ceiling(la.paycheck*r.RPP/25)*25)),1000) as Max_Loan_Limit,
			@list_gen_time as list_generation_time
			from reporting_cf.leads_accepted la
			left join reporting.vw_loan_limit_rates r  
         on la.state=r.state_code and la.loan_sequence = r.loan_sequence and la.pay_frequency = r.pay_frequency 
         -- and la.product=r.product_code
         and (case when la.state='TX' and la.product='IPP' and la.storename like '%BAS%' then 'IPP-BAS'
									 when la.state='TX' and la.product='IPP' and la.storename like '%NCP%' then 'IPP-NCP'
									 else la.product end) =r.product_code  -- DAT-807
			where la.application_status='Pending'
			and la.loan_sequence=1  and la.isreturning = 0
			and date(la.received_time) between @std_date and @end_date
           -- and la.state !='OH' -- DAT-1287
			and IF(@opt_out_YN=1, la.Email_MarketingOptIn=1, la.Email_MarketingOptIn IN (1, 0))
			and SUBSTR(SUBSTR(la.emailaddress, INSTR(la.emailaddress, '@'), INSTR(la.emailaddress, '.')), 2) not in ('epic.lmsmail.com', 'moneykey.com')
			and SUBSTR(SUBSTR(la.emailaddress, INSTR(la.emailaddress, '@'), INSTR(la.emailaddress, '.')), 2) not like 'epic%'
      and la.IsApplicationTest = 0 -- june 19, 2017 DAT-123
      and la.state != 'MD'  -- June 2 
			and la.lms_customer_id not in
					(       select la2.lms_customer_id from reporting_cf.leads_accepted la2
									 where la2.application_status in ('Withdrawn', 'Withdraw', 'Originated', 'Approve') and date(la2.received_time) >=date(la.received_time) )
			;
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
  call reporting_cf.`SP_campaign_list_gen_PA2_CF`;
  call reporting_cf.`SP_campaign_list_gen_DDR_CF`;
	call reporting_cf.`SP_campaign_list_gen_D_WA_CF`;
  call reporting_cf.`SP_campaign_history_ACB_CF`; -- DAT-948
  call reporting_cf.`SP_campaign_history_WAB_CF`;
END;
