DROP PROCEDURE IF EXISTS reporting_cf.SP_campaign_list_gen_DDR_CF1;
CREATE PROCEDURE reporting_cf.`SP_campaign_list_gen_DDR_CF1`()
BEGIN
/******************************************************************************************************************
---    NAME : SP_campaign_list_gen_DDR_CF
---    DESCRIPTION: script for campaign list data population
---    this initial version was created by Joyce in temp schema, 
---    DD/MM/YYYY    By              Comment
---    29/04/2019    Joyce Li        initial version
---    01/07/2019                    DAT-915 update SP to include NON-ACH bypass PSI
********************************************************************************************************************/
	DECLARE NotRunFlag INT DEFAULT 0;
  DECLARE Is_NextWeekDay_Holiday INT DEFAULT 0;
  DECLARE Is_predefined_no_run_day INT DEFAULT 0;
  DECLARE NextBusinessDate DATE;
  
  SET SQL_SAFE_UPDATES=0;
  SET @start = 'Start', @end = 'End', @success = ' succeeded,', @failed = ' failed, returned SQL_STATE = ', @error_msg = ', error message = ', @total_rows = ' total row count = '; 
  SET @process_name = 'SP_campaign_list_gen_DDR_CF', @status_flag_success = 1, @status_flag_failure = 0;
  SET @valuation_date = date_sub(curdate(), interval 1 day); -- may use business date in the future  
  select count(*) into NotRunFlag
		from reporting.vw_DDR_ach_date_matching
		where ori_date = @valuation_date
			and (weekend + holiday) >= 1; -- skip the job on weekend and holiday	
    
	IF NotRunFlag = 0 THEN
		-- log the start info
		CALL reporting.SP_process_log(@valuation_date, @process_name, @start, null, 'job is running', null);
		 
		set
		@channel='email',
		@list_name='Daily DDR',
		@list_module='DDR',
		@list_frq='D',
		@list_gen_time= date_sub(now(), interval 1 day),
		@time_filter='Due Date',
		@opt_out_YN= 0,
		@first_interval= 3;
		set
		@first_date= date (if(weekday(@valuation_date) in (5,6),0,if(weekday(@valuation_date) in (0,1), Date_add(@valuation_date, interval @first_interval day), Date_add(@valuation_date, interval @first_interval+2 day)))),
		@comment='Due Date Reminder for 3 day further date';
		select @channel,@list_name,@list_module,@list_frq,@list_gen_time,@time_filter,@opt_out_YN, @first_interval,@second_interval,@third_interval,@first_date,@second_date,@third_date, @comment;

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
			SET @process_label ='Populate JAGLMS data into campaign_history', @process_type = 'Insert';
				
			INSERT INTO reporting_cf.campaign_history
			(business_date, Channel,   list_name,  job_ID,     list_module,      list_frq,   lms_customer_id,  lms_application_id, received_time,    lms_code,   state,      product,      loan_sequence,    email,      Customer_FirstName,
			Customer_LastName, origination_time, original_loan_amount, ach_date, ach_debit,ach_finance, ach_principal,list_generation_time)

			select distinct
      @valuation_date, 
			@channel,
			@list_name as list_name,
			date_format(@list_gen_time, 'JAG%m%d%YDDR') as job_ID,
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
      la.origination_time as `origination_time`, -- DAT-647
			la.approved_amount as original_loan_amount,
			psi.item_date as ach_date,
			psi.total_amount as ach_debit,
			psi.amount_fee as ach_finance,
			psi.amount_prin as ach_pricipal,
			@list_gen_time as list_generation_time

			from reporting_cf.vcf_lms_base_loans b
			inner join reporting_cf.leads_accepted la on b.customer_id=la.lms_customer_id and la.lms_application_id= b.loan_header_id
			inner join reporting_cf.vcf_lms_payment_schedules ps on ps.base_loan_id=b.base_loan_id
			inner join reporting_cf.vcf_lms_payment_schedule_items psi on psi.payment_schedule_id=ps.payment_schedule_id

			where
			la.lms_code='JAG'

			and IF(@opt_out_YN=1, la.Email_MarketingOptIn=1, la.Email_MarketingOptIn IN (1, 0))
			and ps.is_active=1
			and ps.is_collections=0
			and la.loan_status != 'Default'
			and la.loan_status != 'Paid Off'
			-- and psi.status='scheduled'
      and (psi.status='scheduled' or (psi.payment_mode='NON-ACH' and psi.status='bypass')) -- DAT-915
			and b.is_paying=1
			and date(psi.item_date) in (@first_date, @second_date)
			and psi.total_amount > 0
			and SUBSTR(SUBSTR(la.emailaddress, INSTR(la.emailaddress, '@'), INSTR(la.emailaddress, '.')), 2) not in ('epic.lmsmail.com', 'moneykey.com')
      and la.IsApplicationTest = 0 -- june 19, 2017 DAT-123
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
     
   /*  -- add the daily update for dm_campaign_mapping daily update
		call reporting.SP_dm_campaign_mapping_upd;
     -- call campaign_history_update 
        call reporting.SP_campaign_history_update; */
  -- Check if next weekday is holiday
  SELECT count(*) INTO Is_NextWeekDay_Holiday
    FROM reporting.vw_DDR_ach_date_matching d1
    WHERE d1.ori_date = 
          (SELECT MIN(d2.ori_date)  
            FROM reporting.vw_DDR_ach_date_matching d2
            WHERE d2.Ori_Date > @valuation_date
        	    AND d2.weekend = 0)
      AND d1.weekend = 0 
      AND d1.holiday = 1;
      
 
		 IF Is_NextWeekDay_Holiday = 1 then -- if next business date is holiday, then rollover the business date and generate the DDR
			
				-- get the business date      
				SELECT d1.ori_date into NextBusinessDate 
					FROM reporting.vw_DDR_ach_date_matching d1
					WHERE d1.ori_date = 
								(SELECT MIN(d2.ori_date)  
									FROM reporting.vw_DDR_ach_date_matching d2
									WHERE d2.Ori_Date > @valuation_date
										AND d2.weekend = 0)
						AND d1.weekend = 0 
						AND d1.holiday = 1;
						-- the proess will not run if next day is 'Thanksgiving Day', 'Christmas', 'New Year's Day'
        select count(*) into Is_predefined_no_run_day 
          from jaglms.business_holidays 
          where holiday = NextBusinessDate 
            and description in( 'Thanksgiving Day', 'Christmas', 'New Year''s Day');
				IF Is_predefined_no_run_day = 0 then 
					SET @process_label = CONCAT('Rollover the list generation date to next buseinss date: ',date_format(NextBusinessDate, '%d-%b-%Y'));  

					CALL reporting.SP_process_log(@valuation_date, @process_name, @process_label, null, 'Starting Holiday process', @status_flag_success);
				
					CALL reporting_cf.`SP_campaign_list_gen_DDR_holiday_CF`(NextBusinessDate);
			 END IF;
		 END IF;
  END IF;
  
END;
