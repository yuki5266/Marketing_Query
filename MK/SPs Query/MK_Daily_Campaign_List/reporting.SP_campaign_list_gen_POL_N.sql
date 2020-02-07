DROP PROCEDURE IF EXISTS reporting.SP_campaign_list_gen_POL_N;
CREATE PROCEDURE reporting.`SP_campaign_list_gen_POL_N`()
BEGIN
/********************************************************************************************************************************
---    NAME : SP_campaign_list_gen_POL_N
---    DESCRIPTION: script for campaign list data population
---    this initial version was created by Joyce 
---    DD/MM/YYYY    By              Comment
---    29/11/2018    Eric Pu         DAT-647 add the SP to reportingn schema with exception handling and logs
---    26/04/2019                    DAT-792 remove OH from daily/weekly SPs
---                                  DAT-1118 run everyday
---    16/10/2019										 DAT-1125 update to include POL 90
---    02/01/2020                    DAT-1301 exclude only the following withdrawn reasons:
                                      3 Bad Contact Information
                                      6 Bankruptcy
                                      15 Invalid State Of Residence
                                      21 Possible Fraud
                                      29 Active in the Military
---    03/01/2020 										DAT-1308 update EPIC logic
---    22/01/2020                     DAT-1301 change logic for EPIC population
---    28/01/2020                     DAT-1375 update logic to exclue EPIC TX FP
**********************************************************************************************************************************/
	DECLARE IsHoliday INT DEFAULT 0;
	DECLARE NotRunFlag INT DEFAULT 0;
  SET SQL_SAFE_UPDATES=0;
  SET SESSION tx_isolation='READ-COMMITTED';
  SET @start = 'Start', @end = 'End', @success = ' succeeded,', @failed = ' failed, returned SQL_STATE = ', @error_msg = ', error message = ', @total_rows = ' total row count = '; 
  SET @process_name = 'SP_campaign_list_gen_POL_N', @status_flag_success = 1, @status_flag_failure = 0;
  SET @valuation_date = curdate(); -- may use business date in the future
  SET @MonthNumber = Month(curdate());
  SET @DayNumber = Day(curdate());
  
 /* SELECT count(*) INTO IsHoliday  DAT-1118
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

	IF NotRunFlag = 0 THEN  */
		-- log the start info
		CALL reporting.SP_process_log(@valuation_date, @process_name, @start, null, 'job is running', null);

		set
		@channel='email',
		@list_name='Daily POL',
		@list_module='POL_NEW',
		@list_frq='D',
		@list_gen_time= now(),
		@time_filter='Paid Off Date',
		@opt_out_YN= 1,
		@test_job_id = 'JAG_TEST_POLJ';  
    
    set
		@comment='Paid off loans remarketing';
    
		/*set
		@first_date= date (if(weekday(@list_gen_time) in (5,6),0,if(weekday(@list_gen_time) in (0,1,2), Date_sub(@list_gen_time, interval @first_interval+2 day), Date_sub(@list_gen_time, interval @first_interval day)))),
		@second_date_1= date (if(weekday(@list_gen_time) in (5,6),0,Date_sub(@list_gen_time, interval @second_interval+4 day))), -- DAT-578
    @second_date= date (if(weekday(@list_gen_time) in (5,6),0,Date_sub(@list_gen_time, interval @second_interval+6 day))),
		@third_date= date (if(weekday(@list_gen_time) in (5,6),0,Date_sub(@list_gen_time, interval @third_interval+12 day))),
		@fourth_date= date (if(weekday(@list_gen_time) in (5,6),0,Date_sub(@list_gen_time, interval @fourth_interval+18 day))),
		@comment='Paid off loans remarketing';*/
		-- select @channel, @list_name,@list_module,@list_frq,@list_gen_time,@time_filter,@opt_out_YN, @first_interval,@second_interval_1, @second_interval,@third_interval,@fourth_interval,@first_date,@second_date,@third_date,@fourth_date, @comment;
	/*	BEGIN
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
			SET @process_label ='Populate EPIC data into campaign_history', @process_type = 'Insert';

			INSERT INTO reporting.campaign_history

			(business_date, Channel,       list_name,      job_ID, list_module,    list_frq,       lms_customer_id,        lms_application_id, received_time,      lms_code,       state,  product,        loan_sequence,  email,  Customer_FirstName,
			Customer_LastName,      last_repayment_date,list_generation_time)

			select distinct
      @valuation_date,
			@channel,
			@list_name as list_name,
      '' as job_ID, ###Joyce
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
			-- p.effectivedate as last_repayment_date,
      max(p.effectivedate) as last_repayment_date, -- DAT-1308
			@list_gen_time as list_generation_time
			from reporting.leads_accepted la
			join ais.vw_loans l on la.lms_application_id=if(l.originalloanid=0, l.id, originalloanid)
			inner join ais.vw_payments p on l.id=p.LoanId
			where
			la.lms_code='EPIC'
			and l.loanstatus='Paid Off Loan'
      ##### POL_new Joyce
			and  p.EffectiveDate>=Date_sub(@list_gen_time, interval 91 day)
			and la.state !='OH' -- DAT-792
			and p.PaymentStatus = 'Checked' 
			and p.IsDebit=1
			and IF(@opt_out_YN=1, la.Email_MarketingOptIn=1, la.Email_MarketingOptIn IN (1, 0))


			and la.lms_customer_id not in
					(       select la2.lms_customer_id from reporting.leads_accepted la2
									 where date(la2.origination_time) >=date(la.last_paymentdate)           
											or (la2.application_status='Pending' and date(la2.received_time) >=date(la.last_paymentdate)) ##Joyce 
											or la2.loan_status in ('Returned Item Pending Paid Off','Charged Off Pending Paid Off','Returned Item','Charged Off') 
											or (la2.application_status in ('Withdrawn', 'Withdraw') and date(la2.received_time) >=date(la.last_paymentdate)
													-- and la2.withdrawn_reason_code not in (1,2,10,16,19,22,23,24,25,26,27)
                         and la2.withdrawn_reason_code in (3,6,15,21,29) -- DAT-1301
                          ) 
                          )                                
			and SUBSTR(SUBSTR(la.emailaddress, INSTR(la.emailaddress, '@'), INSTR(la.emailaddress, '.')), 2) not in ('epic.lmsmail.com', 'moneykey.com')
      and la.IsApplicationTest = 0 
      and la.product != 'LOC'  -- DAT-1125
      group by la.lms_customer_id,la.lms_application_id; -- DAT-1301
			-- log the process
			IF sql_code = '00000' THEN
				GET DIAGNOSTICS rowCount = ROW_COUNT;
				SET return_message = CONCAT(@process_type, @success, @total_rows,rowCount);
				CALL reporting.SP_process_log(@valuation_date, @process_name, @process_label, @process_type, return_message, @status_flag_success);
			ELSE
				SET return_message = CONCAT(@process_type, @failed, sql_code, @error_msg ,sql_msg);
				CALL reporting.SP_process_log(@valuation_date, @process_name, @process_label, @process_type, return_message, @status_flag_failure);
			END IF;
		
		END; */ -- DAT-1375
   
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
			SET @process_label ='Populate JAGLMS data into campaign_history', @process_type = 'Insert';

			INSERT INTO reporting.campaign_history

			(business_date, Channel,       list_name,      job_ID, list_module,    list_frq,       lms_customer_id,        lms_application_id, received_time,      lms_code,       state,  product,        loan_sequence,  email,  Customer_FirstName,
			Customer_LastName,      last_repayment_date,list_generation_time)
			select distinct
      @valuation_date,
			@channel,
			@list_name as list_name,
      '' as job_ID, ###Joyce
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
			b.paid_off_date as last_repayment_date,
			@list_gen_time as list_generation_time


			from reporting.leads_accepted la
			inner join jaglms.lms_base_loans b on la.lms_application_id =b.loan_header_id

			where
			la.lms_code='JAG'
			and la.state not in ('LA', 'SD', 'OH') -- DAT-792
			and b.loan_status='Paid Off'
			and date(if(weekday(b.paid_off_date) in (5,6), (select pre_target_date from reporting.vw_DDR_ach_date_matching where Ori_date=b.paid_off_date), b.paid_off_date))>=Date_sub(@list_gen_time, interval 91 day)
			and IF(@opt_out_YN=1, la.Email_MarketingOptIn=1, la.Email_MarketingOptIn IN (1, 0))
			and (SELECT sum(principal_amount*-1) FROM jaglms.lms_client_transactions lct where lct.base_loan_id=b.base_loan_id) = 0
			and la.lms_customer_id not in
					(       select la2.lms_customer_id from reporting.leads_accepted la2
									 where date(la2.origination_time) >=date(la.last_paymentdate)           
											or (la2.application_status='Pending' and date(la2.received_time) >=date(la.last_paymentdate) ) ###Joyce
											or la2.loan_status in ('Returned Item Pending Paid Off','Charged Off Pending Paid Off','Returned Item','Charged Off', 'Charged Off Paid Off') 
											or (la2.application_status in ('Withdrawn', 'Withdraw') and date(la2.received_time) >=date(la.last_paymentdate)
													-- and la2.withdrawn_reason_code not in (1,2,10,16,19,22,23,24,25,26,27)
                          and la2.withdrawn_reason_code in (3,6,15,21,29) -- DAT-1301
                          )  )                                
			and SUBSTR(SUBSTR(la.emailaddress, INSTR(la.emailaddress, '@'), INSTR(la.emailaddress, '.')), 2) not in ('epic.lmsmail.com', 'moneykey.com')
      and la.product != 'LOC' -- DAT-1125
			and (case when la.state='CA' and la.product='SEP' then 1 else 0 end)= 0 ;-- DAT-1289;  

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
  -- END IF;
END;
