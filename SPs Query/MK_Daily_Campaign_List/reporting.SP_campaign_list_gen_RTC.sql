DROP PROCEDURE IF EXISTS reporting.SP_campaign_list_gen_RTC;
CREATE PROCEDURE reporting.`SP_campaign_list_gen_RTC`()
BEGIN
/*********************************************************************************************************************************
---    NAME : SP_RTC_info_daily
---    DESCRIPTION: script for generate RTC data for KS and MO for marketing reports
---    DD/MM/YYYY    By              Comment
---    15/10/2019    Eric Pu         initial verision
***********************************************************************************************************************************/
	DECLARE IsHoliday INT DEFAULT 0;
	DECLARE NotRunFlag INT DEFAULT 0;
  SET SQL_SAFE_UPDATES=0;
  SET @start = 'Start', @end = 'End', @success = ' succeeded,', @failed = ' failed, returned SQL_STATE = ', @error_msg = ', error message = ', @total_rows = ' total row count = '; 
  SET @process_name = 'SP_campaign_list_gen_RTC', @status_flag_success = 1, @status_flag_failure = 0;
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
			SET @process_label ='populate campaign list data for RTC', @process_type = 'Insert';

       insert into reporting.`rtc_campaign_list` (
							`report_date`,
							`lms_customer_id` ,
							`email_address`,
							`customer_full_name`,
							`origination_date`,
							`loc_amount`,
							`due_date`,
							`cure_date`,
							`statement_date`,
							`minimum_payment_due`,
							`state`,
							`lms_application_id` ,
							`original_lead_id` ,
							`loan_status`,
							`days_delinquent`,
							`agent_name`,
							`agent_extension`,
							`list_generation_date`,
							`day_name`,
							`full_date`)
				select current_date() report_date, 
								la.lms_customer_id, 
								la.emailaddress,  
								concat(ucase(substring(la.customer_firstname,1,1)), lower(substring(la.customer_firstname,2)), ' ', 
											 ucase(substring(la.customer_lastname,1,1)), lower(substring(la.customer_lastname,2))) as customer_full_name,
								date(la.origination_time) as origination_date,
								bsp.CreditLimit as loc_amount,
								date_sub(curdate(), interval bsp.DaysDelinquent day) as due_date,
								date_add(curdate(), interval 21 day) as cure_date,
								sh.StatementDate, 
								sh.MinimumPaymentDue,
								la.state,
								la.lms_application_id, la.original_lead_id, la.loan_status, bsp.DaysDelinquent,
								'Olivia Harper' as agent_name,
								'4122' as agent_extension,
								NOW() as list_generation_date,
								dayname(CURDATE()),
								DATE_FORMAT(curdate(), "%W , %M %e , %Y")
						from LOC_001.ca_BSegment_Primary bsp
						inner join reporting.leads_accepted la on trim(leading 0 from bsp.AccountNumber)=la.lms_customer_id and 
																											la.lms_code='TDC' and la.origination_time>0 and la.state in('KS', 'MO')
						left join LOC_001.ca_StatementHeader sh on bsp.AccountNumber=sh.AccountNumber and date_sub(curdate(), interval bsp.DaysDelinquent day)
																																															=date(sh.DateOfTotalDue)                                        
						where bsp.SystemStatus=3 and bsp.CCinHParent125AID=2
									and (if(weekday(curdate())=0, bsp.DaysDelinquent in (11,12,13), bsp.DaysDelinquent=11));
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
