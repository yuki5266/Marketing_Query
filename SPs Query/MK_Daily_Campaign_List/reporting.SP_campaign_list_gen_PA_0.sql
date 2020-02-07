DROP PROCEDURE IF EXISTS reporting.SP_campaign_list_gen_PA_0;
CREATE PROCEDURE reporting.`SP_campaign_list_gen_PA_0`(intervaldays int)
BEGIN
/*********************************************************************************************************************************
---    NAME : SP_campaign_list_gen_PA_0
---    DESCRIPTION: script for campaign list data population
---    DD/MM/YYYY    By              Comment
---    02/05/2017    Eric Pu         based on the logic from Norman, adding exception handling and logs
---    09/04/2019    Eric Pu         DAT-768 Add SMS Flag inro campaign history
									 DAT-770 Add Phone Information into campaign history
---    26/04/2019                    DAT-792 remove OH from daily/weekly SPs
---    28/06/2019       						 DAT-912 update the where condition to  if(la.loan_sequence<=7, la.loan_sequence, 7) = r.loan_sequence
																		  to handle the loan sequnce great than 7
---    23/12/2019										 DAT-1289 Update Email Campaign SPs -to exclude CA & SC
************************************************************************************************************************************/
DECLARE IsHoliday INT DEFAULT 0;
DECLARE NotRunFlag INT DEFAULT 0;
SET SQL_SAFE_UPDATES=0;
SET SESSION tx_isolation='READ-COMMITTED';
SET @start = 'Start', @end = 'End', @success = ' succeeded,', @failed = ' failed, returned SQL_STATE = ', @error_msg = ', error message = ', @total_rows = ' total row count = '; 
SET @process_name = 'SP_campaign_list_gen_PA_0', @status_flag_success = 1, @status_flag_failure = 0;
SET @valuation_date = curdate();  
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
		@list_name='L0 Daily Pending',
		@list_module='PA_0',
		@list_frq='D',
		@list_gen_time= curdate(),
		@time_filter='Lead_Received_Date',
		@opt_out_YN= 0,
		@before_interval=0, 
		@after_interval= 0,
		@test_job_id = 'JAG_TEST_PAJ';
    
		set
		@std_date= if(weekday(@list_gen_time) in (5,6),0,(select Operation_Date from reporting.vw_DDR_ach_date_matching where Ori_date=date(Date_sub(@list_gen_time, interval @before_interval day)))),
		@end_date= if(weekday(@list_gen_time) in (5,6),0,date(Date_add(@list_gen_time, interval @after_interval day))),
		@comment='Pending Application received today before 3pm';
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
			SET @process_label ='Main process to populate NC data into campaign_history', @process_type = 'Insert';
			
			INSERT INTO reporting.campaign_history
			(business_date, Channel,       list_name,      job_ID, list_module,    list_frq,       lms_customer_id,       
      lms_application_id, received_time,      lms_code,       state,  product,        loan_sequence,  email,  Customer_FirstName,
			Customer_LastName,      key_word, Req_Loan_Amount,      max_loan_limit,list_generation_time)

			select DISTINCT
			@valuation_date,
			@channel,
			@list_name as list_name,
			case
			when la.lms_code ='TDC' then date_format(curdate(), '%m%d%YPAT') 
			when la.lms_code ='JAG' then date_format(curdate(), '%m%d%YPAJ')
			when la.lms_code ='EPIC' then date_format(curdate(), '%m%d%YPA')
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
			(case when lms_code='JAG' and product = 'PD' then 'application'   
            when lms_code='JAG' and product = 'SEP' then  'Installment Loan application'
            when lms_code='TDC'  then  'Line of Credit application'
            else 'application' end) as key_word, 
			ifnull(la.requested_amount, la.approved_amount) as Req_Loan_Amount, 
			ifnull(least(r.hardcap,(ceiling(la.paycheck*r.RPP/25)*25)),1000) as Max_Loan_Limit,
			@list_gen_time as list_generation_time

			from reporting.leads_accepted la
			left join reporting.vw_loan_limit_rates r on la.state=r.state_code and la.loan_sequence = r.loan_sequence and la.pay_frequency = r.pay_frequency and la.product=r.product_code
			where la.application_status='Pending'
			and la.loan_sequence=1  and la.isreturning = 0
			and date(la.received_time)=curdate() 
			and hour(la.received_time) < 15  -- only inlcude leads received before 3 pm
			and IF(@opt_out_YN=1, la.Email_MarketingOptIn=1, la.Email_MarketingOptIn IN (1, 0))
			and SUBSTR(SUBSTR(la.emailaddress, INSTR(la.emailaddress, '@'), INSTR(la.emailaddress, '.')), 2) not in ('epic.lmsmail.com', 'moneykey.com')
			and SUBSTR(SUBSTR(la.emailaddress, INSTR(la.emailaddress, '@'), INSTR(la.emailaddress, '.')), 2) not like 'epic%'
			and la.IsApplicationTest = 0  
      and la.state not in('MD','OH','SC','CA') 
			-- and la.state != 'MD'  
      -- and la.state !='OH' -- DAT-792
			and la.lms_customer_id not in
					(select la2.lms_customer_id from reporting.leads_accepted la2
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
 
     -- incldue RC data
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
			SET @process_label ='Main process to populate RC data into campaign_history', @process_type = 'Insert';
			
			INSERT INTO reporting.campaign_history
			(business_date, Channel,       list_name,      job_ID, list_module,    list_frq,       lms_customer_id,        lms_application_id, received_time,      lms_code,       state,  product,        loan_sequence,  email,  Customer_FirstName,
			Customer_LastName,      key_word, Req_Loan_Amount,      max_loan_limit,list_generation_time)

			select DISTINCT
			@valuation_date,
			@channel,
			@list_name as list_name,
			case
			when la.lms_code ='TDC' then date_format(curdate(), '%m%d%YPAT') 
			when la.lms_code ='JAG' then date_format(curdate(), '%m%d%YPAJ')
			when la.lms_code ='EPIC' then date_format(curdate(), '%m%d%YPA')
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
			(case when lms_code='JAG' and product = 'PD' then 'application'   
            when lms_code='JAG' and product = 'SEP' then  'Installment Loan application'
            when lms_code='TDC'  then  'Line of Credit application'
            else 'application' end) as key_word, 
			ifnull(la.requested_amount, la.approved_amount) as Req_Loan_Amount, 
			ifnull(least(r.hardcap,(ceiling(la.paycheck*r.RPP/25)*25)),1000) as Max_Loan_Limit,
			@list_gen_time as list_generation_time

			from reporting.leads_accepted la
			left join reporting.vw_loan_limit_rates r on la.state=r.state_code 
      -- and la.loan_sequence = r.loan_sequence 
      and if(la.loan_sequence<=7, la.loan_sequence, 7) = r.loan_sequence -- DAT-912
      and la.pay_frequency = r.pay_frequency 
      -- and la.product=r.product_code
      and (case when la.state='TX' and la.product='IPP' and la.storename like '%BAS%' then 'IPP-BAS'
									 when la.state='TX' and la.product='IPP' and la.storename like '%NCP%' then 'IPP-NCP'
									 else la.product end) =r.product_code  -- DAT-807
			where la.application_status='Pending'
			and la.loan_sequence>1  and la.isreturning = 1
			and date(la.received_time)=curdate()
			and hour(la.received_time) < 15  -- only inlcude leads received before 3 pm
			and IF(@opt_out_YN=1, la.Email_MarketingOptIn=1, la.Email_MarketingOptIn IN (1, 0))
			and SUBSTR(SUBSTR(la.emailaddress, INSTR(la.emailaddress, '@'), INSTR(la.emailaddress, '.')), 2) not in ('epic.lmsmail.com', 'moneykey.com')
			and SUBSTR(SUBSTR(la.emailaddress, INSTR(la.emailaddress, '@'), INSTR(la.emailaddress, '.')), 2) not like 'epic%'
			and la.IsApplicationTest = 0 -- june 19, 2017 DAT-123
      and la.state not in('MD','OH','SC')
			-- and la.state != 'MD'  -- June 2 
            -- and la.state !='OH' -- DAT-792
			and la.lms_customer_id not in
					(       select la2.lms_customer_id from reporting.leads_accepted la2
									 where la2.application_status in ('Withdrawn', 'Withdraw', 'Originated', 'Approve') and date(la2.received_time) >=date(la.received_time) )
                   and (case when la.state='CA' and la.product='SEP' then 1 else 0 end)=0
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
	-- DAT-768/DAT-770
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
			SET @process_label ='populate the EPIC phone number into campaign_history', @process_type = 'update';
			
			update reporting.campaign_history ch
			inner join ais.vw_client vc on ch.lms_customer_id=vc.Id
            set ch.home_phone = vc.HomePhone, ch.cell_phone = vc.CellPhone 
			where ch.list_module = @list_module
              and ch.lms_code = 'EPIC' and ch.business_date >= curdate();
			
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
			SET @process_label ='populate the TDC phone number into campaign_history', @process_type = 'update';
			
			update reporting.campaign_history ch
			inner join LOC_001.ca_Customer tc on ch.lms_customer_id= tc.Cust_ID
            set ch.home_phone = tc.Cust_HPhone, ch.cell_phone = tc.Cust_Mphone
			where ch.list_module = @list_module
              and ch.lms_code = 'TDC' and ch.business_date >= curdate();
 
            
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
			SET @process_label ='populate the JAG phone number into campaign_history', @process_type = 'update';
			
			update reporting.campaign_history ch
			inner join jaglms.lms_customer_info_flat c on ch.lms_customer_id= c.customer_id
            set ch.home_phone = c.homephone, ch.cell_phone = c.cellphone
			where ch.list_module = @list_module
              and ch.lms_code = 'JAG' and ch.business_date >= curdate();
 
            
			IF sql_code = '00000' THEN
				GET DIAGNOSTICS rowCount = ROW_COUNT;
				SET return_message = CONCAT(@process_type, @success, @total_rows,rowCount);
				CALL reporting.SP_process_log(@valuation_date, @process_name, @process_label, @process_type, return_message, @status_flag_success);
			ELSE
				SET return_message = CONCAT(@process_type, @failed, sql_code, @error_msg ,sql_msg);
				CALL reporting.SP_process_log(@valuation_date, @process_name, @process_label, @process_type, return_message, @status_flag_failure);
			END IF;
		
		END;
   -- populate the Is_Transactional_optin and Is_SMS_Marketing_optin flag
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
			SET @process_label ='populate the SMS flag into campaign_history', @process_type = 'Update';   

       update reporting.campaign_history ch
				inner join (select list.customer_id, 
                count(if(list.notification_name = 'SMS_TRANSACTIONAL' and list.state = 1, list.customer_id, null)) as 'Transactional With Consent',
                count(if(list.notification_name = 'SMS_TRANSACTIONAL' and list.Txt_Stop = 1, list.customer_id, null)) as 'Transactional Text Stop',   
                count(if(list.notification_name = 'SMS_MARKETING' and list.state = 1, list.customer_id, null)) as 'SMS Marketing With Consent', 
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
        group by list.customer_id) aa on ch.lms_customer_id = aa.customer_id
	    set ch.is_transactional_optin = if(aa.`Transactional With Consent`=1 and aa.`Transactional Text Stop`=0,1,0),
            ch.is_sms_marketing_optin = if(aa.`SMS Marketing With Consent`=1 and  aa.`SMS Marketing Text Stop`=0,1,0)
			where ch.list_module = @list_module
              and ch.lms_code = 'JAG' and ch.business_date >= curdate();
 
   
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
