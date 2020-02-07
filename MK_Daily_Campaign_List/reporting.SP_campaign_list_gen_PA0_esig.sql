DROP PROCEDURE IF EXISTS reporting.SP_campaign_list_gen_PA0_esig;
CREATE PROCEDURE reporting.`SP_campaign_list_gen_PA0_esig`()
BEGIN
/*********************************************************************************************************************************
---    NAME : SP_campaign_list_gen_PA0_esig
---    DESCRIPTION: script for campaign list data population
---    DD/MM/YYYY    By              Comment
---    21/03/2019    Eric Pu         based on the logic from DAT-754, adding exception handling and logs
---    25/03/2019                    add is_econsent flag
---    26/04/2019                    DAT-792 remove OH from daily/weekly SPs
---    23/12/2019										 DAT-1289 Update Email Campaign SPs -to exclude CA & SC
************************************************************************************************************************************/
DECLARE IsHoliday INT DEFAULT 0;
DECLARE NotRunFlag INT DEFAULT 0;
SET SQL_SAFE_UPDATES=0;
SET SESSION tx_isolation='READ-COMMITTED';
SET @start = 'Start', @end = 'End', @success = ' succeeded,', @failed = ' failed, returned SQL_STATE = ', @error_msg = ', error message = ', @total_rows = ' total row count = '; 
SET @process_name = 'SP_campaign_list_gen_PA0_esig', @status_flag_success = 1, @status_flag_failure = 0;
SET @valuation_date = curdate();  
SET @MonthNumber = Month(curdate());
SET @DayNumber = Day(curdate());
SELECT HOUR(CURTIME()) INTO @runhour;
SELECT WEEKDAY(CURTIME()) INTO @weekday;
  
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
		@list_name='L0 Daily Pending No Esig',
		@list_module='PA0_no_esig',
		@list_frq='D',
		@list_gen_time= now(),
		@time_filter='Lead_Received_Date',
		@opt_out_YN= 0,
		@before_interval=0, 
		@after_interval= 0,
		@test_job_id = 'JAG_TEST_PAJ';
    
		set
		@std_date= if(weekday(@list_gen_time) in (5,6),0,(select Operation_Date from reporting.vw_DDR_ach_date_matching where Ori_date=date(Date_sub(@list_gen_time, interval @before_interval day)))),
		@end_date= if(weekday(@list_gen_time) in (5,6),0,date(Date_add(@list_gen_time, interval @after_interval day))),
		@comment='Pending Application without esig received today before 6:30pm';
		-- select @list_name,@list_module,@list_frq,@list_gen_time,@time_filter,@opt_out_YN, @before_interval,@after_interval,@std_date,@end_date, @comment;
        
    IF @weekday < 5 THEN -- weekday only
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
			SET @process_label ='populate PA0_no_esig data', @process_type = 'Insert';
			
			INSERT INTO reporting.campaign_history_esig

			(business_date, Channel, list_name, job_ID, list_module, list_frq,  lms_customer_id, lms_application_id, received_time,lms_code, 
            state,product,loan_sequence, email,Customer_FirstName,customer_LastName, key_word, Req_Loan_Amount, max_loan_limit,list_generation_time,
            application_status, is_DM, is_originated, todo_item_cnt, incomplete_cnt, is_esig_incomplete, final_cell_phone, is_transactional_optin, is_sms_marketing_optin)
         
           select @valuation_date,
			@channel,
			@list_name as list_name,
			case
			when ap.lms_code ='TDC' then date_format(curdate(), '%m%d%YPAT') 
			when ap.lms_code ='JAG' then date_format(curdate(), '%m%d%YPAJ')
			when ap.lms_code ='EPIC' then date_format(curdate(), '%m%d%YPA')
			end as job_ID,  
			@list_module as list_module,
			@list_frq as list_frq,           
            ap.lms_customer_id,
			ap.lms_application_id,
			ap.received_time,
			ap.lms_code,
			ap.state,
			ap.product,
			ap.loan_sequence,
			ap.email,
            ap.Customer_FirstName,
            ap.Customer_LastName,
            (case when lms_code='JAG' and product = 'PD' then 'application'   
            when lms_code='JAG' and product = 'SEP' then  'Installment Loan application'
            when lms_code='TDC'  then  'Line of Credit application'
            else 'application' end) as key_word, 
            ap.Req_Loan_Amount,
            ap.Max_Loan_Limit,
            @list_gen_time as list_generation_time,
            ap.application_status,
            ap.is_DM,
            ap.is_originated, ap.todo_item_cnt, ap.incomplete_cnt, ap.is_esig_incomplete,
            if(lcif.cellphone is null or lcif.cellphone<=0 or lcif.cellphone='(999)999-9999', lcif.homephone, lcif.cellphone) as Final_Cell_Phone,
			if(marketing.`Transactional With Consent`=1 and  marketing.`Transactional Text Stop`=0,1,0) as Is_Transactional_optin,
			if(marketing.`SMS Marketing With Consent`=1 and  marketing.`SMS Marketing Text Stop`=0,1,0) as Is_SMS_Marketing_optin 			 
			from (select DISTINCT
				la.lms_customer_id,
				la.lms_application_id,
				la.original_lead_id,
				la.received_time,
				la.lms_code,
				la.state,
				la.product,
				la.loan_sequence,
				la.emailaddress as email,
				CONCAT(UCASE(SUBSTRING(la.customer_firstname, 1, 1)),LOWER(SUBSTRING(la.customer_firstname, 2))) as Customer_FirstName,
				CONCAT(UCASE(SUBSTRING(la.customer_lastname, 1, 1)),LOWER(SUBSTRING(la.customer_lastname, 2))) as Customer_LastName,			
				ifnull(la.requested_amount, la.approved_amount) as Req_Loan_Amount, 
				ifnull(least(r.hardcap,(ceiling(la.paycheck*r.RPP/25)*25)),1000) as Max_Loan_Limit,
				curdate() as list_generation_time,
				la.application_status,
				if(la.campaign_name like '%DM%',1,0) as is_DM,
				if(la.origination_loan_id>0,1,0) as is_originated,
				
				count(distinct wf.todo_name) as todo_item_cnt, 
				sum(if(wf.completion_time is null,1,0)) as incomplete_cnt,
				 (select sum(if(wf2.completion_time is null,1,0)) from jaglms.loan_header_todo_list wf2 where wf2.loan_header_id=wf.loan_header_id  and wf2.todo_name='e-sig' limit 1) as is_esig_incomplete
					from reporting.leads_accepted la
					left join reporting.vw_loan_limit_rates r on la.state=r.state_code and la.loan_sequence = r.loan_sequence and la.pay_frequency = r.pay_frequency and la.product=r.product_code
				left join jaglms.loan_header_todo_list wf on la.lms_application_id=wf.loan_header_id 
					where la.application_status='Pending'
          AND la.state not in ('MD','OH','SC','CA') -- need to update this condition to exclude 'CA' on 2019-12-31
                    -- and la.state !='OH' -- DAT-792
					and la.received_time >=curdate() 
					and IF(@opt_out_YN=1, la.Email_MarketingOptIn=1, la.Email_MarketingOptIn IN (1, 0))
					and SUBSTR(SUBSTR(la.emailaddress, INSTR(la.emailaddress, '@'), INSTR(la.emailaddress, '.')), 2) not in ('epic.lmsmail.com', 'moneykey.com')
					and SUBSTR(SUBSTR(la.emailaddress, INSTR(la.emailaddress, '@'), INSTR(la.emailaddress, '.')), 2) not like 'epic%'
				and la.IsApplicationTest = 0 
				and la.lms_code='JAG'
					and la.lms_customer_id not in
							(select la2.lms_customer_id from reporting.leads_accepted la2
								where la2.application_status in ('Withdrawn', 'Withdraw', 'Originated', 'Approve') and date(la2.received_time) >=date(la.received_time) )
					group by la.lms_code, la.lms_customer_id, la.lms_application_id) ap
			  left join jaglms.lms_customer_info_flat lcif on ap.lms_customer_id=lcif.customer_id 
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
					group by list.customer_id) marketing on ap.lms_customer_id=marketing.customer_id
			  where ap.incomplete_cnt=1 and ap.is_esig_incomplete=1;
              
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
     END IF;
     
     -- populate PA0_esig_NBV
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
			SET @process_label ='populate PA0_esig_NBV data', @process_type = 'Insert';
            -- set @list_module = 'PA0_esig_NBV'; 
			INSERT INTO reporting.campaign_history_esig
			(business_date, Channel, list_name, job_ID, list_module, list_frq,  lms_customer_id, lms_application_id, received_time,lms_code, 
            state,product,loan_sequence, email,Customer_FirstName,customer_LastName, key_word, -- Req_Loan_Amount, max_loan_limit,
            list_generation_time, origination_time,withdrawn_time, loan_number,  
             pay_frequency, esig_wf, paystub_wf,  bV_wf, Spouse_wf, emp_wf, other_wf,  ref_wf, tnc_wf , is_transactional_optin, econsent, is_econsent,
             bv_final, no_bank_call, iS_HB_LMS_ABA_Diff,iS_HB_LMS_BankAccountNumber_Diff )         
           select @valuation_date,
			@channel,
			@list_name,
			case
			when c.lms_code ='TDC' then date_format(curdate(), '%m%d%YPAT') 
			when c.lms_code ='JAG' then date_format(curdate(), '%m%d%YPAJ')
			when c.lms_code ='EPIC' then date_format(curdate(), '%m%d%YPA')
			end as job_ID,  
			'PA0_esig_NBV' as list_module,
			@list_frq as list_frq,           
			c.lms_customer_id, c.lms_application_id,
            c.received_time, c.lms_code, c.state, c.product, c.loan_sequence, c.email, c.Customer_FirstName, c.Customer_LastName, '' as key_word, 
            now() as list_generation_time, c.origination_time, c.withdrawn_time, 
			c.loan_number,   c.pay_frequency, c.esig_wf, c.paystub_wf, c.BV_wf, c.Spouse_wf,
			c.emp_wf, c.other_wf,  c.ref_wf, c.tnc_wf, c.sms_consent as is_transactional_optin, 
			(case when c.state = 'WI' then c.EconsentWI else c.Econsent end) as Econsent,
            (case when (case when c.state = 'WI' then c.EconsentWI else c.Econsent end) is not null then 1 else 0 end) as is_econsent, -- 25/03/2019
			 (case when c.no_bank_call = 0 then 'BV'
			 when c.no_bank_call = 1 and (c.IS_HB_LMS_ABA_Diff =1 or  c.IS_HB_LMS_BankAccountNumber_Diff = 1) then 'BV'
			 when c.no_bank_call = 1 and (c.IS_HB_LMS_ABA_Diff =0 and  c.IS_HB_LMS_BankAccountNumber_Diff = 0) then 'NBV'
			 else null end) as BV_Final,
			 c.no_bank_call,
			 c.IS_HB_LMS_ABA_Diff,
			 c.IS_HB_LMS_BankAccountNumber_Diff
			from
			(select la.lead_sequence_id, la.received_time, la.origination_time, la.withdrawn_time, la.lms_code, la.loan_number, la.lms_customer_id,  la.lms_application_id, 
            la.state,  la.pay_frequency, la.no_bank_call, la.product, la.loan_sequence, la.emailaddress as email,
            CONCAT(UCASE(SUBSTRING(la.customer_firstname, 1, 1)),LOWER(SUBSTRING(la.customer_firstname, 2))) as Customer_FirstName,
  			CONCAT(UCASE(SUBSTRING(la.customer_lastname, 1, 1)),LOWER(SUBSTRING(la.customer_lastname, 2))) as Customer_LastName,
			ifnull(
			abs(
			STRCMP(
			(Select f2.routingnumber from jaglms.lms_customer_info_flat f2 where f2.customer_id=la.lms_customer_id and la.lms_code = 'JAG' limit 1)
			, 
			dw.routingnumber)
			), 'MISSING')
			as IS_HB_LMS_ABA_Diff,

			ifnull(
			abs(
			STRCMP(
			(Select f.account_number from jaglms.lms_customer_info_flat f where f.customer_id=la.lms_customer_id and la.lms_code = 'JAG' limit 1)
			, 
			SUBSTRING_INDEX(CONVERT(AES_DECRYPT(dw.bankaccount,'09qewkjlnasdfiuasdjnq2r09iqweklmnagu0q92310x109cm901c212cn9129rn9hr'),CHAR(100)),':',-1))
			), 'MISSING')
			as IS_HB_LMS_BankAccountNumber_Diff,

			 ifnull((select sum(if(wf2.completion_time is null,0,1)) from jaglms.loan_header_todo_list wf2 where wf2.loan_header_id=la.lms_application_id  and wf2.todo_name='e-sig' limit 1),0) as esig_wf,
			 ifnull((select sum(if(wf2.completion_time is null,0,1)) from jaglms.loan_header_todo_list wf2 where wf2.loan_header_id=la.lms_application_id   and wf2.todo_name='Bank Verification' limit 1),0) as BV_Wf,
			  ifnull( (select sum(if(wf2.completion_time is null,0,1)) from jaglms.loan_header_todo_list wf2 where wf2.loan_header_id=la.lms_application_id  and wf2.todo_name='Spouse Info' limit 1),0) as Spouse_wf,
				 ifnull(  (select sum(if(wf2.completion_time is null,0,1)) from jaglms.loan_header_todo_list wf2 where wf2.loan_header_id=la.lms_application_id and wf2.todo_name='Read and  Agreed to T&C' limit 1),0) as tnc_wf,
				 ifnull(  (select sum(if(wf2.completion_time is null,0,1)) from jaglms.loan_header_todo_list wf2 where wf2.loan_header_id=la.lms_application_id  and wf2.todo_name='Other loan Amount' limit 1),0) as other_wf,
				 ifnull(  (select sum(if(wf2.completion_time is null,0,1)) from jaglms.loan_header_todo_list wf2 where wf2.loan_header_id=la.lms_application_id  and wf2.todo_name='References' limit 1) ,0)as ref_wf,
				ifnull(   (select sum(if(wf2.completion_time is null,0,1)) from jaglms.loan_header_todo_list wf2 where wf2.loan_header_id=la.lms_application_id  and wf2.todo_name='Employment Verification Info' limit 1),0) as emp_wf,
				ifnull(   (select sum(if(wf2.completion_time is null,0,1)) from jaglms.loan_header_todo_list wf2 where wf2.loan_header_id=la.lms_application_id  and wf2.todo_name='Paystub' limit 1),0) as paystub_wf,
						
			ifnull((select cn.state from jaglms.lms_customer_notifications cn
			 inner join jaglms.lms_notification_name_mapping nnm on cn.notification_name_id = nnm.notification_name_mapping_id
			 where cn.customer_id = la.lms_customer_id and nnm.notification_name = 'SMS_TRANSACTIONAL'),0) as SMS_Consent,
			(select max(wi.create_timestamp) from webapi.signed_document wi where wi.document_name = 'EConsent-WI'and wi.base_loan_id = la.loan_number limit 1) as EconsentWI,
			(select max(a.create_timestamp)  from webapi.signed_document a where a.document_name = 'EConsent' and a.base_loan_id = la.loan_number limit 1) as Econsent
			from reporting.leads_accepted la 
			left join datawork.mk_application dw
			on dw.lead_sequence_id = la.lead_sequence_id
			left join jaglms.lms_customer_info_flat cf
			on cf.customer_id = la.lms_customer_id
			where date(la.received_time) = curdate() and la.isreturning = 0 and la.isexpress = 0
      AND la.state not in ('MD','OH','SC','CA') -- need to update this condition to exclude 'CA' on 2019-12-31
            -- and la.state !='OH' -- DAT-792
             ) c
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

	 -- log the process for completion
		CALL reporting.SP_process_log(@valuation_date, @process_name, @end, null, 'job is done', @status_flag_success);
  END IF;
 
END;
