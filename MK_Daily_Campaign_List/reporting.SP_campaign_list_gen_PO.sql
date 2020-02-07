DROP PROCEDURE IF EXISTS reporting.SP_campaign_list_gen_PO;
CREATE PROCEDURE reporting.`SP_campaign_list_gen_PO`()
BEGIN
/*********************************************************************************************************************************
---    NAME : SP_campaign_list_gen_PO
---    DESCRIPTION: script for campaign list data population
---    this initial version was created by Joyce in temp schema, 
---    DD/MM/YYYY    By              Comment
---    02/05/2017    Eric Pu         migrate it to reportingn schema
---																	 added exception handling and logs
---    01/06/2017    Eric Pu         DAT-71 - change the SP running schedule based on holiday table
--- 																 will run on every weekday except New Year (Jan 1), Christmas (Dec 25) and US Thanksgiving Day
---    07/06/2017    Eric Pu         DAT-72 add business_date column
---    20/06/2017    Eric Pu         DAT-123 change SP to use the IsApplicationTest flag in leads_accepted, also add tester email for the 
---                                  email process verification 
---    12/09/2019                    DAT-1046 add next_loan_limit
***********************************************************************************************************************************/
	DECLARE IsHoliday INT DEFAULT 0;
	DECLARE NotRunFlag INT DEFAULT 0;
  SET SQL_SAFE_UPDATES=0;
  SET SESSION tx_isolation='READ-COMMITTED';
  SET @start = 'Start', @end = 'End', @success = ' succeeded,', @failed = ' failed, returned SQL_STATE = ', @error_msg = ', error message = ', @total_rows = ' total row count = '; 
  SET @process_name = 'SP_campaign_list_gen_PO', @status_flag_success = 1, @status_flag_failure = 0;
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
		@list_name='Pending Paid Off',
		@list_module='PO',
		@list_frq='D',
		@list_gen_time= now(),
		@time_filter='Due Date',
		@opt_out_YN= 0,
		@first_interval= 0, 
    @test_job_id = 'JAG_TEST_POJ';    

		set
		@first_date= if(weekday(@list_gen_time) in (5,6),0,date (Date_sub(@list_gen_time, interval @first_interval day))),
		@comment='To be Paid off on the day';
		select @list_name,@list_module,@list_frq,@list_gen_time,@time_filter,@opt_out_YN, @first_interval,@first_date, @comment;
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
			SET @process_label ='Step1 process to populate data into campaign_history', @process_type = 'Insert';
		
			INSERT INTO reporting.campaign_history

			(business_date, Channel,       list_name,      job_ID, list_module,    list_frq,       lms_customer_id,        lms_application_id, received_time,      lms_code,       state,  product,        loan_sequence,  email,  Customer_FirstName,
			Customer_LastName,      ach_date, ach_debit,list_generation_time, next_loan_limit
			)
      
			select distinct
      @valuation_date,
			@channel,
			@list_name as list_name,
			if(la.state='SD',date_format(@list_gen_time, '%m%d%YPOSD'),date_format(@list_gen_time, '%m%d%YPO'))  as job_ID,
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
			p.EffectiveDate as ach_date,
			p.PaymentAmount as ach_debit,
			@list_gen_time as list_generation_time,
      least(ceiling(vp.TotalPerPaycheck*dd.RPP/25)*25,dd.hardcap) as NEXT_LOAN_LIMIT      
			from ais.vw_loans l
			join reporting.leads_accepted la on  la.lms_customer_id=l.debtorclientid and la.lms_application_id=if(l.OriginalLoanId=0,l.id,l.OriginalLoanId)
			join ais.vw_payments p on p.LoanId=l.Id
      left join ais.vw_payroll vp on la.lms_customer_id=vp.clientId and la.lms_code='EPIC'
      left join reporting.vw_loan_limit_rates dd on la.product = dd.product_code and la.state = dd.state_code and  
      (case when la.loan_sequence+1>7 then 7
             when la.loan_sequence+1<=7 then la.loan_sequence+1 end)= dd.loan_sequence and 
       (case when vp.frequencytype='Bi-Weekly' then 'B'
             when vp.frequencytype='Semi-Monthly' then 'S'
             when vp.frequencytype='Weekly' then 'W'
             when vp.frequencytype='Monthly' then 'M' end) = dd.pay_frequency
        
			where
			l.loanstatus='Pending Paid Off' and
			date(p.EffectiveDate) = @first_date and
			p.PaymentStatus='NotChecked' and
			p.isdebit=1 and
			l.CollectionStartDate is null and
			IF(@opt_out_YN=1, la.Email_MarketingOptIn=1, la.Email_MarketingOptIn IN (1, 0))and
			SUBSTR(SUBSTR(la.emailaddress, INSTR(la.emailaddress, '@'), INSTR(la.emailaddress, '.')), 2) not in ('epic.lmsmail.com', 'moneykey.com')
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
			SET @process_label ='Step2 process to populate data into campaign_history', @process_type = 'Insert';


			INSERT INTO reporting.campaign_history

			(business_date, Channel,       list_name,      job_ID, list_module,    list_frq,       lms_customer_id,        lms_application_id, received_time,      lms_code,       state,  product,        loan_sequence,  email,  Customer_FirstName,
			Customer_LastName,      ach_date, ach_debit, list_generation_time, next_loan_limit
			)
			select distinct
      @valuation_date,
			@channel,
			@list_name as list_name,
			if(la.state='SD',date_format(@list_gen_time, '%m%d%YPOSDJ'),date_format(@list_gen_time, '%m%d%YPOJ'))  as job_ID,
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
			tr.trans_date as ach_date,
			tr.amount as ach_debit,
			@list_gen_time as list_generation_time,
     case when la.product='PD' then 255
            when lcif.payfrequency='B' then least(ceiling((lcif.nmi/2.16667)*dd.RPP/25)*25,dd.hardcap)
            when lcif.payfrequency='S' then least(ceiling((lcif.nmi/2)*dd.RPP/25)*25,dd.hardcap)
            when lcif.payfrequency='W' then least(ceiling((lcif.nmi/4.3333)*dd.RPP/25)*25,dd.hardcap)
            when lcif.payfrequency='M' then least(ceiling((lcif.nmi/1)*dd.RPP/25)*25,dd.hardcap)
            else null
     end as NEXT_LOAN_LIMIT

			from reporting.leads_accepted la
			inner join jaglms.lms_base_loans b on la.lms_application_id =b.loan_header_id
			inner join jaglms.lms_client_transactions tr on b.base_loan_id=tr.base_loan_id
      inner join jaglms.lms_customer_info_flat lcif on la.lms_customer_id=lcif.customer_id
      left join reporting.vw_loan_limit_rates dd on (
                                                    case when la.state='TX' and la.product='IPP' and la.storename like '%BAS%' then 'IPP-BAS'
                                                         when la.state='TX' and la.product='IPP' and la.storename like '%NCP%' then 'IPP-NCP'
                                                         else la.product end) = dd.product_code 
                                       and la.state = dd.state_code 
                                       and   (case when la.loan_sequence+1>7 then 7
                                       when la.loan_sequence+1<7 then la.loan_sequence+1 end)= dd.loan_sequence and lcif.payfrequency = dd.pay_frequency

			where la.loan_status = 'Pending Paid Off'
			and  tr.trans_type in ('Debit', 'D')
			and date(tr.trans_date)=@first_date
			and date(tr.trans_date) >= date(la.last_paymentdate)
			and IF(@opt_out_YN=1, la.Email_MarketingOptIn=1, la.Email_MarketingOptIn IN (1, 0))
			and SUBSTR(SUBSTR(la.emailaddress, INSTR(la.emailaddress, '@'), INSTR(la.emailaddress, '.')), 2) not in ('epic.lmsmail.com', 'moneykey.com')
			and cast(la.loan_number as unsigned) not in (select distinct base_loan_id from jaglms.collection_lms_loan_map)
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
