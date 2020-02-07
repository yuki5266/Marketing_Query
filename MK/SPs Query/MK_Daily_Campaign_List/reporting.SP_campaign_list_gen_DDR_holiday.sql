DROP PROCEDURE IF EXISTS reporting.SP_campaign_list_gen_DDR_holiday;
CREATE PROCEDURE reporting.`SP_campaign_list_gen_DDR_holiday`()
BEGIN
/******************************************************************************************************************
---    NAME : SP_campaign_list_gen_DDR_holiday
---    DESCRIPTION: script for campaign list data population if next weekday is holiday
---    DD/MM/YYYY    By              Comment
---    07/06/2017    Eric Pu         DAT-72 Automate the DDR Campaign data generation if next weekday is holiday
---    20/06/2017    Eric Pu         DAT-123 change SP to use the IsApplicationTest flag in leads_accepted, also add tester email for the 
---                                  email process verification 
--     02/11/2017                    DAT-250  Change DDR10 to DDR9 
---    25/07/2018                    DAT-511 Add the job_id for any TN clients be "ALOC"
---    29/11/2018                    DAT-647 change the job id for JAG KS LOC; add origination time
---    01/07/2019                    DAT-915 update SP to include NON-ACH bypass PSI
---    06/08/2019                    DAT-965 add ddr_type and origination_loan_id 
********************************************************************************************************************/
 	DECLARE RunFlag INT DEFAULT 0;
  SET SQL_SAFE_UPDATES=0;
  SET @start = 'Start', @end = 'End', @success = ' succeeded,', @failed = ' failed, returned SQL_STATE = ', @error_msg = ', error message = ', @total_rows = ' total row count = '; 
  SET @process_name = 'SP_campaign_list_gen_DDR_holiday', @status_flag_success = 1, @status_flag_failure = 0;
	SET @valuation_date = p_business_date;
  select count(*) into RunFlag
		from reporting.vw_DDR_ach_date_matching
		where ori_date = @valuation_date
			and weekend = 0 and holiday = 1; -- only run if the weekday is holiday
	IF RunFlag = 1 THEN
		-- log the start info
		CALL reporting.SP_process_log(@valuation_date, @process_name, @start, null, 'job is running', null);
		 
    set
		@channel='email',
		@list_name='Daily DDR',
		@list_module='DDR',
		@list_frq='D',
		@list_gen_time= now(),
		@time_filter='Due Date',
		@opt_out_YN= 0,
		@first_interval= 3, 
		@second_interval= 11, 
		@third_interval= 7;
		set
		@first_date= date (if(weekday(@valuation_date) in (5,6),0,if(weekday(@valuation_date) in (0,1), Date_add(@valuation_date, interval @first_interval day), Date_add(@valuation_date, interval @first_interval+2 day)))),
		@second_date= date (if(weekday(@valuation_date) in (5,6),0,if(weekday(@valuation_date) =0, Date_add(@valuation_date, interval @second_interval day), Date_add(@valuation_date, interval @second_interval+2 day)))),
		@third_date= date (if(weekday(@valuation_date) in (5,6),0,Date_add(@valuation_date, interval @third_interval day))),
		@comment='Due Date Reminder for 3 or 5 or 9 day further date';
		select @channel,@list_name,@list_module,@list_frq,@list_gen_time,@time_filter,@opt_out_YN, @first_interval,@second_interval,@third_interval,@first_date,@second_date,@third_date, @comment;

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
			(business_date, Channel,   list_name,  job_ID,     list_module,      list_frq,   lms_customer_id,  lms_application_id, received_time,    lms_code,   state,      product,      loan_sequence,    email,      Customer_FirstName,
			Customer_LastName, origination_time, original_loan_amount, ach_date, ach_debit,ach_finance, ach_principal,list_generation_time, ddr_type, origination_loan_id)

			select distinct
      @valuation_date,
			@channel,
			@list_name as list_name,
      (case when la.state = 'TN' then date_format(@list_gen_time, 'ALOC%m%d%YDDR') else date_format(@list_gen_time, 'JAG%m%d%YDDR') end) as job_ID,
			-- date_format(@list_gen_time, 'JAG%m%d%YDDR') as job_ID,
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
			@list_gen_time as list_generation_time,
			if(psi.item_date =@first_date, 'DDR3', if(psi.item_date = @second_date, 'DDR9', '')) as ddr_type, -- DAT-965
      b.base_loan_id as origination_loan_id
			from jaglms.lms_base_loans b
			inner join reporting.leads_accepted la on b.customer_id=la.lms_customer_id and la.lms_application_id= b.loan_header_id
			inner join jaglms.lms_payment_schedules ps on ps.base_loan_id=b.base_loan_id
			inner join jaglms.lms_payment_schedule_items psi on psi.payment_schedule_id=ps.payment_schedule_id

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
		  SET @process_label ='Populate EPIC OH data into campaign_history', @process_type = 'Insert';
				
			INSERT INTO reporting.campaign_history

			(business_date, Channel,   list_name,  job_ID,     list_module,      list_frq,   lms_customer_id,  lms_application_id, received_time,    lms_code,   state,      product,      loan_sequence,    email,      Customer_FirstName,
			Customer_LastName,origination_time, original_loan_amount, ach_date, ach_debit,ach_finance, ach_principal,list_generation_time, ddr_type, origination_loan_id)

			select distinct
      @valuation_date,
			@channel,
			@list_name as list_name,
			date_format(@list_gen_time, 'EOH%m%d%YDDR') as job_ID,
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
			l.DueDate as ach_date,
			l.approvedamount + l.approvedfinancefee as ach_debit,
			l.approvedfinancefee as ach_finance,
			l.approvedamount as ach_principal,
			@list_gen_time as list_generation_time,
      if(p.effectivedate =@first_date, 'DDR3', if(p.effectivedate = @second_date, 'DDR9', '')) as ddr_type, -- DAT-965
      la.lms_application_id as origination_loan_id
			from ais.vw_loans l
			join ais.vw_payments p on p.LoanId=l.Id
			join reporting.leads_accepted la on la.lms_customer_id=l.debtorclientid and la.lms_application_id=if(l.OriginalLoanId=0,l.id,l.OriginalLoanId)

			where
			la.loan_status not in ('Pending Application', 'DELETED','Denied','Withdrawn Application', 'Voided New Loan',
			'Voided Renewed Loan','Paid Off Loan', 'Returned Item') and
			la.collection_startdate is null and
			l.loanstatus not in ('Pending Application','Denied','Withdrawn Application', 'Voided New Loan', 'Voided Renewed Loan','Paid Off Loan', 'Returned Item') and
			l.CollectionStartDate is null and
			l.DueDate > DATE(@list_gen_time) and
			p.IsDebit=1
			and p.EffectiveDate=@third_date
			and p.paymentstatus in ('Pending','None')
			and  p.paymentstatus !='DELETED'
			and la.state = 'OH'
			and IF(@opt_out_YN=1, la.Email_MarketingOptIn=1, la.Email_MarketingOptIn IN (1, 0))
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
			SET @process_label ='Populate EPIC Non-OH data into campaign_history', @process_type = 'Insert';
				 
			INSERT INTO reporting.campaign_history

			(business_date, Channel,   list_name,  job_ID,     list_module,      list_frq,   lms_customer_id,  lms_application_id, received_time,    lms_code,   state,      product,      loan_sequence,    email,      Customer_FirstName,
			Customer_LastName, origination_time, original_loan_amount, ach_date, ach_debit,total_outstanding,list_generation_time, ddr_type, origination_loan_id)

			select distinct
      @valuation_date,
			@channel,
			@list_name as list_name,
			date_format(@list_gen_time, 'ETX%m%d%YDDR') as job_ID,
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
			p.EffectiveDate as ach_date,
			p.PaymentAmount as ach_debit,
			l.approvedamount + l.approvedfinancefee as Total_Outstanding,
			@list_gen_time as list_generation_time,
      if(p.effectivedate =@first_date, 'DDR3', if(p.effectivedate = @second_date, 'DDR9', '')) as ddr_type, -- DAT-965
      la.lms_application_id as origination_loan_id

			from ais.vw_loans l
			join ais.vw_payments p on p.LoanId=l.Id
			join reporting.leads_accepted la on la.lms_customer_id=l.debtorclientid and la.lms_application_id=if(l.OriginalLoanId=0,l.id,l.OriginalLoanId)

			where
			la.loan_status not in ('Pending Application', 'DELETED','Denied','Withdrawn Application', 'Voided New Loan',
			'Voided Renewed Loan','Paid Off Loan', 'Returned Item')
			and la.collection_startdate is null
			and l.loanstatus not in ('Pending Application','Denied','Withdrawn Application', 'Voided New Loan', 'Voided Renewed Loan','Paid Off Loan', 'Returned Item')
			and l.CollectionStartDate is null
			and l.State !='OH'

			and p.effectivedate  in (@first_date, @second_date)
			and l.DueDate > DATE(@list_gen_time)
			and p.IsDebit=1
			and p.paymentstatus in ('Pending','None')
			and  p.paymentstatus !='DELETED'
			and IF(@opt_out_YN=1, la.Email_MarketingOptIn=1, la.Email_MarketingOptIn IN (1, 0))
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
			SET @process_label ='Populate TDC data into campaign_history', @process_type = 'Insert';
				 
			INSERT INTO reporting.campaign_history

			(business_date, Channel,   list_name,  job_ID,     list_module,      list_frq,   lms_customer_id,  lms_application_id, received_time,    lms_code,   state,      product,      loan_sequence,    email,      Customer_FirstName,
			Customer_LastName, origination_time, ach_date, ach_debit,list_generation_time, ddr_type, origination_loan_id)

			select distinct
      @valuation_date, 
			@channel,
			@list_name as list_name,
			Case when la.state='MD' and date(la.origination_time) <'2016-05-24' then date_format(@list_gen_time, 'BMD%m%d%YDDR')
					 when la.state!='MD' and date(la.origination_time) <'2016-05-24' then date_format(@list_gen_time, 'BLOC%m%d%YDDR')
					 WHEN date(la.origination_time) >='2016-05-24' then date_format(@list_gen_time, 'ALOC%m%d%YDDR')
					 ELSE 'ERROR'
					 END as job_ID,
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
			if(pr.IsAfterHoliday=1,(select adm.post_target_date from reporting.vw_DDR_ach_date_matching adm where date(p.DateOfTotalDue)=adm.ori_date), (select adm.pre_target_date from reporting.vw_DDR_ach_date_matching adm where date(p.DateOfTotalDue)=adm.ori_date)) as ach_date,
			p.AmtOfPayCurrDue as ach_debit,
			@list_gen_time as list_generation_time,
      if(if(pr.IsAfterHoliday=1,(select adm.post_target_date from reporting.vw_DDR_ach_date_matching adm where date(p.DateOfTotalDue)=adm.ori_date), 
      (select adm.pre_target_date from reporting.vw_DDR_ach_date_matching adm where date(p.DateOfTotalDue)=adm.ori_date)) =@first_date, 'DDR3', 
      if(if(pr.IsAfterHoliday=1,(select adm.post_target_date from reporting.vw_DDR_ach_date_matching adm where date(p.DateOfTotalDue)=adm.ori_date), 
      (select adm.pre_target_date from reporting.vw_DDR_ach_date_matching adm where date(p.DateOfTotalDue)=adm.ori_date)) = @second_date, 'DDR9', '')) as ddr_type, -- DAT-965
      la.lms_application_id as origination_loan_id

			from reporting.leads_accepted la
			inner join LOC_001.ca_BSegment_Primary p on la.lms_customer_id=p.AccountNumber and la.origination_time is not null
			inner join LOC_001.ca_BSegment_Secondary bss on p.acctId=bss.acctId
			inner join LOC_001.ca_Payroll pr on p.AccountNumber=pr.cust_ID


			where DATE(if(pr.IsAfterHoliday=1,(select adm.post_target_date from reporting.vw_DDR_ach_date_matching adm where date(p.DateOfTotalDue)=adm.ori_date), (select adm.pre_target_date from reporting.vw_DDR_ach_date_matching adm where date(p.DateOfTotalDue)=adm.ori_date))) in (@first_date, @second_date)
			and la.State != 'VA'
			and p.AmtOfPayCurrDue !=0
			and bss.AllowACHProcssing=1
			and IF(@opt_out_YN=1, la.Email_MarketingOptIn=1, la.Email_MarketingOptIn IN (1, 0))
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
        -- 20/09/2017
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
			SET @process_label ='Populate DDR5 into campaign_history', @process_type = 'Insert';
			INSERT INTO reporting.campaign_history
                  (business_date, Channel,   list_name,  job_ID,     list_module,      list_frq,   lms_customer_id,  lms_application_id, received_time,    lms_code,   state,      product,      loan_sequence,    email,      Customer_FirstName,
                  Customer_LastName,origination_time, original_loan_amount, ach_date, ach_debit,ach_finance, ach_principal,list_generation_time,
									original_cso_fee, number_of_installment,rebate_cso_amount, ach_interest, total_outstanding_finance_fee_pif, pif_amount, ddr_type, origination_loan_id)

                  select distinct
									@valuation_date, 
                  @channel,
                  @list_name as list_name,
                  (case when la.state = 'TN' then date_format(@list_gen_time, 'ALOC%m%d%YDDR5') else date_format(@list_gen_time, 'JAG%m%d%YDDR5') end) as job_ID,
                  -- date_format(@list_gen_time, 'JAG%m%d%YDDR5') as job_ID,
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
                  @list_gen_time as list_generation_time,
									case when  
									b.lms_entity_id in (47, 48) and cl.capitalized_fees =0 then la.approved_amount*2
									when b.lms_entity_id in (45, 49) and cl.capitalized_fees =0 then la.approved_amount*1.2
									else cl.capitalized_fees
									end as new_capitalized_fees,

									if(la.pay_frequency='M',4, if(la.pay_frequency in ('S', 'W', 'B'), 14, " ")) as no_installments,
									-- concat(format(r.refund_percent, 2),'%') as Rebate_Rate,



									(case when 
									b.lms_entity_id in (47, 48) and cl.capitalized_fees =0 then la.approved_amount*2
									when b.lms_entity_id in (45, 49) and cl.capitalized_fees =0 then la.approved_amount*1.2
									else cl.capitalized_fees end)*(r.refund_percent/100) as CSO_PIF_Rebate_Amount,


									psi.amount_int as ach_interest,

									-- (SELECT count(distinct item_date)+1 FROM jaglms.lms_payment_schedule_items psi2 WHERE  psi2.payment_schedule_id=psi.payment_schedule_id  
									-- and psi2.total_amount>0 -- item_type='C', the total amount is negative
									-- and psi2.status in ('Cleared','SENT', 'Pending','Return','MISSED','Correction') and psi2.item_date<psi.item_date) as `cycle_number`,

									((select sum(-1*fee_amount) from jaglms.lms_client_transactions lct where lct.base_loan_id=b.base_loan_id)-(case when 
									b.lms_entity_id in (47, 48) and cl.capitalized_fees =0 then la.approved_amount*2
									when b.lms_entity_id in (45, 49) and cl.capitalized_fees =0 then la.approved_amount*1.2
									else cl.capitalized_fees end)*(r.refund_percent/100)) as Total_outstanding_fee_PIF,

									((select sum(-1*principal_amount) from jaglms.lms_client_transactions lct where lct.base_loan_id=b.base_loan_id)+psi.amount_int+((select sum(-1*fee_amount) from jaglms.lms_client_transactions lct where lct.base_loan_id=b.base_loan_id)-(case when -- la.product='SEP' and la.state='OH' and 
									b.lms_entity_id in (47, 48) and cl.capitalized_fees =0 then la.approved_amount*2
									when b.lms_entity_id in (45, 49) and cl.capitalized_fees =0 then la.approved_amount*1.2
									else cl.capitalized_fees end)*(r.refund_percent/100)))
									as PIF_Amount,
                  if(psi.item_date =@first_date, 'DDR3', if(psi.item_date = @second_date, 'DDR9', '')) as ddr_type, -- DAT-965
									b.base_loan_id as origination_loan_id
									from jaglms.lms_base_loans b
									inner join reporting.leads_accepted la on b.customer_id=la.lms_customer_id and la.lms_application_id= b.loan_header_id
									inner join jaglms.lms_payment_schedules ps on ps.base_loan_id=b.base_loan_id
									inner join jaglms.lms_payment_schedule_items psi on psi.payment_schedule_id=ps.payment_schedule_id
									left join jaglms.lms_entities e on b.lms_entity_id=e.lms_entity_id
									left join jaglms.entity_fee_refund_schedule r on r.entity_id=e.lms_entity_id and r.timespan=(SELECT count(distinct item_date)+1 FROM jaglms.lms_payment_schedule_items psi2 WHERE  psi2.payment_schedule_id=psi.payment_schedule_id  
									and psi2.total_amount>0 
									and psi2.status in ('Cleared','SENT', 'Pending','Return','MISSED','Correction') and psi2.item_date<psi.item_date)
									left join jaglms.cso_loans cl on b.base_loan_id=cl.base_loan_id

									where
									la.lms_code='JAG' 
									and la.state='OH'

									and IF(@opt_out_YN=1, la.Email_MarketingOptIn=1, la.Email_MarketingOptIn IN (1, 0))
									and ps.is_active=1
									and ps.is_collections=0
									and la.loan_status != 'Default'
									and la.loan_status != 'Paid Off'
									-- and psi.status='scheduled'
                  and (psi.status='scheduled' or (psi.payment_mode='NON-ACH' and psi.status='bypass')) -- DAT-915
									and b.is_paying=1
									and date(psi.item_date) =@third_date
									and psi.total_amount > 0
									and SUBSTR(SUBSTR(la.emailaddress, INSTR(la.emailaddress, '@'), INSTR(la.emailaddress, '.')), 2) not in ('epic.lmsmail.com', 'moneykey.com')
									and la.IsApplicationTest = 0
									and (SELECT count(distinct item_date)+1 FROM jaglms.lms_payment_schedule_items psi2 WHERE  psi2.payment_schedule_id=psi.payment_schedule_id  
									and psi2.total_amount>0 -- item_type='C', the total amount is negative
									and psi2.status in ('Cleared','SENT', 'Pending','Return','MISSED','Correction') and psi2.item_date<psi.item_date)=1
									;	
     
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
