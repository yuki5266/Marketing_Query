DROP PROCEDURE IF EXISTS reporting.SP_campaign_list_gen_TDCLOCGC;
CREATE PROCEDURE reporting.`SP_campaign_list_gen_TDCLOCGC`()
BEGIN
/*********************************************************************************************************************************
---    NAME : SP_campaign_list_gen_TDCLOCGC
---    DESCRIPTION: script for monthly_campaign_history data population
-- from Dec,2018 on, we will also include active account <60% avaialable credit but with more than $150 available amount
/************************************************
LOC GC criteria: 
For MD, KS and MO
Active: No default
        >=60%available_credit_limit or >=$150
        last topup date is 30 days ago.
Inactive: No default
        last payment date is 15 days ago
        last topup date is 15 days ago.
************************************************* 
  
---    DD/MM/YYYY    Ticket#        Comment
---    15/10/2019    DAT-1037 			initial version 
---    18/12/2019 	 DAT-1284				remove state in where clause
************************************************************************************************************************************/
	DECLARE IsHoliday INT DEFAULT 0;
	DECLARE NotRunFlag INT DEFAULT 0;
  SET SQL_SAFE_UPDATES=0;
  SET @start = 'Start', @end = 'End', @success = ' succeeded,', @failed = ' failed, returned SQL_STATE = ', @error_msg = ', error message = ', @total_rows = ' total row count = '; 
  SET @process_name = 'SP_campaign_list_gen_TDCLOCGC', @status_flag_success = 1, @status_flag_failure = 0;
  SET @valuation_date = curdate();  
	SET @MonthNumber = Month(curdate());
  SET @DayNumber = Day(curdate());
  SET @intervaldays = 30;  
	-- SET @std_date= @std_date=(select subdate(curdate(),@intervaldays)), @end_date= curdate();
	SET @std_date= '2013-01-01',@end_date= curdate();
  
/*  SELECT count(*) INTO IsHoliday
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

	IF NotRunFlag = 0 THEN */
		-- log the start info
		CALL reporting.SP_process_log(@valuation_date, @process_name, @start, null, 'job is running', null);

	-- prepare temporary tables 
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
			SET @process_label ='Prepare data in temporary tables', @process_type = 'Insert';	

			DROP TEMPORARY TABLE IF EXISTS table1;
			CREATE TEMPORARY TABLE IF NOT EXISTS table1 ( INDEX(lms_customer_id) ) 
			AS (
			select la.lms_customer_id, 
			la.lms_application_id,
			la.state,
			la.customer_firstname as FirstName, 
			la.customer_lastname as LastName, 
			la.pay_frequency,
			max(la.emailaddress) as Email,
			date_format(la.origination_time,'%Y-%m-%d') as origination_date,
			sum(if(tr.paymentstatus is null and tr.TranType='Credit', amount, 0)) as total_draw_amount,
			sum(if(tr.paymentstatus is null and tr.TranType='Credit', 1, 0)) as total_draw_count,

			max(if(tr.paymentstatus is null and tr.TranType in('Debit','Write-Off'), tr.effectivedate, 0)) as last_payment_date,
			-- sum(if(tr.TranType in('Debit','Write-Off'),1,0)) as install_num,
			sum(if(tr.returndate is not null and tr.TranType='Debit', 1, 0)) as total_default_count,
			sum(if(tr.IsCollection is not null, tr.IsCollection, 0)) as total_collection_count,
			bsp.CurrentBalance+bss.currentbalanceco as Final_Total_Balance,
			bsp.Principal+bss.principalco as Final_Principal_Balance,
			bsp.CreditLimit,
			la.loan_status,
			-- max(if(tr.paymentstatus is null and tr.TranType='Credit', tr.effectivedate, 0)) as last_draw_date,
			-- max(tr.CycleNumber) as last_rew_num,
			-- sum(tr.IsCollection) as Col_Num,
			-- SystemStatus,
			ccinhparent125AID
			from reporting.tdc_transactions tr
			join reporting.leads_accepted la on tr.AccountNumber=la.lms_customer_id and la.IsApplicationTest=0
			join LOC_001.ca_BSegment_Primary bsp on tr.AccountNumber=bsp.AccountNumber
			join LOC_001.ca_BSegment_Secondary bss on bsp.acctid = bss.acctid
			where lms_code='TDC' 
			and la.isoriginated=1
			and la.origination_time between @std_date and @end_date
			and la.state in ('KS', 'MO', 'MD')
			and la.loan_status in ('Inactive', 'Active')
			and bsp.SystemStatus in (2,4)   -- exclusion of bad history customer (collection, closed...)
			and bsp.ccinhparent125AID not in (16,1002)  -- exclusion of bad history customer (manual collection, closed...)
			GROUP BY la.lms_customer_id
			);


			/*Region: Exclusion rule1 (Email Marketing Opt Out Customer) */

			DROP TEMPORARY TABLE IF EXISTS exc1;
			CREATE TEMPORARY TABLE IF NOT EXISTS exc1 ( INDEX(lms_customer_id) ) 
			AS (
			select distinct t1.lms_customer_id
			from table1 t1
			join LOC_001.ca_Customer_Flags cf on t1.lms_customer_id = cf.Cust_ID
			where (cf.flag_id=6 and flag_value=1)
			);

			-- select * from temp.table1;
			-- select * from temp.exc1;
			/* Application of Exclusion Rule and Formatting */

			DROP TEMPORARY TABLE IF EXISTS final;
			CREATE TEMPORARY TABLE IF NOT EXISTS final ( INDEX(lms_customer_id) ) 
			AS (
			select 
			t1.lms_customer_id, 
			t1.lms_application_id,
			t1.state,
			t1.pay_frequency,
			CONCAT(UCASE(SUBSTRING(t1.FirstName, 1, 1)),LOWER(SUBSTRING(t1.FirstName, 2))) as Customer_FirstName,
			CONCAT(UCASE(SUBSTRING(t1.LastName, 1, 1)),LOWER(SUBSTRING(t1.LastName, 2))) as Customer_LastName,
			t1.Email,
			t1.origination_date,
			t1.total_draw_amount,
			t1.total_draw_count,
			-- t1.total_repayment,
			-- t1.total_principal_payment,
			-- t1.total_fee_payment,
			t1.total_default_count as HistDefault_Count,
			t1.total_collection_count as HistCollection_Count,
			t1.Final_Total_Balance as total_balance,
			t1.Final_Principal_Balance as principal_balance,
			t1.Final_Total_Balance-t1.Final_Principal_Balance as fee_balance,
			t1.CreditLimit as current_credit_limit,
			t1.CreditLimit-t1.Final_Total_Balance as available_credit_limit,
			t1.loan_status,
			-- date_format(t1.last_draw_date,'%m-%d-%Y') as last_draw_date,
			date_format(t1.last_payment_date,'%Y-%m-%d') as last_payment_date,
			datediff(curdate(),t1.last_payment_date) as Day_since_Paid_Off
			-- ,t1.install_num
			from table1 t1
			left join exc1 e1 on t1.lms_customer_id=e1.lms_customer_id
			where e1.lms_customer_id is null
			);


			DROP TEMPORARY TABLE IF EXISTS final_last_topup;
			CREATE TEMPORARY TABLE IF NOT EXISTS final_last_topup ( INDEX(lms_customer_id) ) 
			AS (
			select f.*, max(tr.EffectiveDate) as last_topup_date
			from final f 
			left join reporting.tdc_transactions tr on f.lms_customer_id=tr.AccountNumber and tr.TranType='Credit' and tr.ReturnDate is null
			group by f.lms_customer_id);

		  IF sql_code = '00000' THEN
				GET DIAGNOSTICS rowCount = ROW_COUNT;
				SET return_message = CONCAT(@process_type, @success, @total_rows,rowCount);
				CALL reporting.SP_process_log(@valuation_date, @process_name, @process_label, @process_type, return_message, @status_flag_success);
			ELSE
				SET return_message = CONCAT(@process_type, @failed, sql_code, @error_msg ,sql_msg);
				CALL reporting.SP_process_log(@valuation_date, @process_name, @process_label, @process_type, return_message, @status_flag_failure);
			END IF;
		END;	
    
    -- populate data into target table
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
			SET @process_label ='add data into target table - Active loan Paid Off over 30 days', @process_type = 'Insert';


			INSERT INTO reporting.loc_gc_campaign_history
			(list_generation_date, list_module, job_ID, lms_customer_id, first_name, email, state, pay_frequency, original_approved_amount,
			total_draw_count, total_draw_amount, origination_date, last_payment_date, total_balance, principal_balance, fee_balance, max_loan_limit, 
			available_credit_limit, loan_status, Day_since_last_payment, avail_credit_rate, Available_credit_range, Day_since_last_topup)
			-- Active
			select
			now() as list_generation_date,
			'TDCLOCGC' as list_module,
			date_format(now(), 'LOC%m%d%YGC') as job_ID,
			f.lms_customer_id, f.Customer_FirstName as FirstName, 
			-- f.Customer_LastName,max(la.age) as Customer_Age, 
			f.email, f.state, 
			la.pay_frequency,
			-- la.lms_code, la.product, 
			la.approved_amount original_approved_amount,
			f.total_draw_count, f.total_draw_amount, f.origination_date, 
			f.last_payment_date, 
			-- datediff(f.last_payment_date,f.origination_date) as customer_total_duration_day,
			-- f.install_num as total_install_count,
			-- f.total_principal_payment,
			-- f.total_fee_payment,
			f.total_balance,
			f.principal_balance,
			f.fee_balance,
			f.current_credit_limit as max_loan_limit,
			f.available_credit_limit,
			f.loan_status,
			f.Day_since_Paid_Off as Day_since_last_payment,
			(f.available_credit_limit/f.current_credit_limit) as avail_credit_rate,
			case when f.available_credit_limit>=f.current_credit_limit then '100%'
					 when (f.available_credit_limit>=0.9*f.current_credit_limit) and (f.available_credit_limit<1*f.current_credit_limit)  then '90%-99%'
					 when (f.available_credit_limit>=0.8*f.current_credit_limit) and (f.available_credit_limit<0.9*f.current_credit_limit)  then '80%-89%'
					 when (f.available_credit_limit>=0.7*f.current_credit_limit) and (f.available_credit_limit<0.8*f.current_credit_limit)  then '70%-79%'
					 when (f.available_credit_limit>=0.6*f.current_credit_limit) and (f.available_credit_limit<0.7*f.current_credit_limit)  then '60%-69%'
					 else '<60%'
			 end as Available_credit_range 
			 , datediff(curdate(),f.last_topup_date) as Day_since_last_topup
			from reporting.leads_accepted la
			join final_last_topup f on la.lms_customer_id=f.lms_customer_id and la.lms_code='TDC'
			where la.isoriginated=1
			and f.HistDefault_Count=0
			and f.HistCollection_Count=0
			-- and f.principal_balance<=5
			### new rules since Dec2018
			and (f.available_credit_limit >=0.6*f.current_credit_limit or(f.available_credit_limit <0.6*f.current_credit_limit and available_credit_limit>=150))
			#
			and f.total_draw_count>0
			and f.loan_status='active'
			-- and f.Day_since_Paid_Off>=30
			and datediff(curdate(),f.last_topup_date)>=30
			-- and f.state!='MD' DAT-1284
			group by f.lms_customer_id;

		IF sql_code = '00000' THEN
				GET DIAGNOSTICS rowCount = ROW_COUNT;
				SET return_message = CONCAT(@process_type, @success, @total_rows,rowCount);
				CALL reporting.SP_process_log(@valuation_date, @process_name, @process_label, @process_type, return_message, @status_flag_success);
			ELSE
				SET return_message = CONCAT(@process_type, @failed, sql_code, @error_msg ,sql_msg);
				CALL reporting.SP_process_log(@valuation_date, @process_name, @process_label, @process_type, return_message, @status_flag_failure);
			END IF;
		END;	

   -- populate data into target table
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
			SET @process_label ='add data into target table - Inactive loan Paid Off over 15 days', @process_type = 'Insert';
        
			INSERT INTO reporting.loc_gc_campaign_history
			(list_generation_date, list_module, job_ID, lms_customer_id, first_name, email, state, pay_frequency, original_approved_amount,
			total_draw_count, total_draw_amount, origination_date, last_payment_date, total_balance, principal_balance, fee_balance, max_loan_limit, 
			available_credit_limit, loan_status, Day_since_last_payment, avail_credit_rate, Available_credit_range, Day_since_last_topup)
			-- Inactive
			select
			now() as list_generation_date,
			'TDCLOCGC' as list_module,
			date_format(now(), 'LOC%m%d%YGC') as job_ID,
			f.lms_customer_id, f.Customer_FirstName as FirstName, 
			-- f.Customer_LastName,max(la.age) as Customer_Age, 
			f.email, f.state, 
			la.pay_frequency,
			-- la.lms_code, la.product, 
			la.approved_amount original_approved_amount,
			f.total_draw_count, f.total_draw_amount, f.origination_date, 
			f.last_payment_date, 
			-- datediff(f.last_payment_date,f.origination_date) as customer_total_duration_day,
			-- f.install_num as total_install_count,
			-- f.total_principal_payment,
			-- f.total_fee_payment,
			f.total_balance,
			f.principal_balance,
			f.fee_balance,
			f.current_credit_limit as max_loan_limit,
			f.available_credit_limit,
			f.loan_status,
			f.Day_since_Paid_Off,
			(f.available_credit_limit/f.current_credit_limit) as avail_credit_rate,
			case when f.available_credit_limit>=f.current_credit_limit then '100%'
					 when (f.available_credit_limit>=0.9*f.current_credit_limit) and (f.available_credit_limit<1*f.current_credit_limit)  then '90%-99%'
					 when (f.available_credit_limit>=0.8*f.current_credit_limit) and (f.available_credit_limit<0.9*f.current_credit_limit)  then '80%-89%'
					 when (f.available_credit_limit>=0.7*f.current_credit_limit) and (f.available_credit_limit<0.8*f.current_credit_limit)  then '70%-79%'
					 when (f.available_credit_limit>=0.6*f.current_credit_limit) and (f.available_credit_limit<0.7*f.current_credit_limit)  then '60%-69%'
					 else '<60%'
			 end as Available_credit_range 
				, datediff(curdate(),f.last_topup_date) as Day_since_last_topup
			from reporting.leads_accepted la
			join final_last_topup f on la.lms_customer_id=f.lms_customer_id and la.lms_code='TDC'
			where la.isoriginated=1
			and f.HistDefault_Count=0
			and f.HistCollection_Count=0
			and f.principal_balance<=5
			-- and f.available_credit_limit >=0.6*f.current_credit_limit
			and f.total_draw_count>0
			and f.Day_since_Paid_Off>=15
			and f.loan_status='Inactive'
			 and datediff(curdate(),f.last_topup_date)>15   -- add this condition because the loan status sometimes is less updated
			 -- and f.state!='MD' DAT-1284
			group by f.lms_customer_id;



			/*select date_Format(list_generation_date, '%m/%d/%Y') as list_generation_date, list_module, job_ID, lms_customer_id, first_name, email, state, pay_frequency, original_approved_amount, 
			total_draw_count, total_draw_amount, origination_date, last_payment_date, total_balance, principal_balance, fee_balance, max_loan_limit,
			available_credit_limit, loan_status, Day_since_last_payment, avail_credit_rate, Available_credit_range, Day_since_last_topup
			from reporting.loc_gc_campaign_history 
			where date(list_generation_date)=curdate() and list_module='TDCLOCGC';*/

			IF sql_code = '00000' THEN
				GET DIAGNOSTICS rowCount = ROW_COUNT;
				SET return_message = CONCAT(@process_type, @success, @total_rows,rowCount);
				CALL reporting.SP_process_log(@valuation_date, @process_name, @process_label, @process_type, return_message, @status_flag_success);
			ELSE
				SET return_message = CONCAT(@process_type, @failed, sql_code, @error_msg ,sql_msg);
				CALL reporting.SP_process_log(@valuation_date, @process_name, @process_label, @process_type, return_message, @status_flag_failure);
			END IF;
		END;	
		CALL reporting.SP_process_log(@valuation_date, @process_name, @end, null, 'job is done', @status_flag_success);
  -- END IF;
END;
