DROP PROCEDURE IF EXISTS reporting_cf.SP_campaign_list_gen_JAGLOCGC_CF;
CREATE PROCEDURE reporting_cf.`SP_campaign_list_gen_JAGLOCGC_CF`()
BEGIN
/*********************************************************************************************************************************
---    NAME : SP_campaign_list_gen_JAGLOCGC_CF
---    DESCRIPTION: script for monthly_campaign_history data population
---    SP will be scheduled to run at 11:30am on 1st and 3rd Wednesday every month
  
---    DD/MM/YYYY    Ticket#        Comment
---    04/02/2020    DAT-1403 			initial version 
---
************************************************************************************************************************************/
	DECLARE IsHoliday INT DEFAULT 0;
	DECLARE NotRunFlag INT DEFAULT 0;
  SET SQL_SAFE_UPDATES=0;
  SET @start = 'Start', @end = 'End', @success = ' succeeded,', @failed = ' failed, returned SQL_STATE = ', @error_msg = ', error message = ', @total_rows = ' total row count = '; 
  SET @process_name = 'SP_campaign_list_gen_JAGLOCGC_CF', @status_flag_success = 1, @status_flag_failure = 0;
  SET @valuation_date = curdate();  
	SET @MonthNumber = Month(curdate());
  SET @DayNumber = Day(curdate());
  -- SET @intervaldays = 30;  SET @std_date= @std_date=(select subdate(curdate(),@intervaldays)),
  SET @std_date= '2019-06-11', @end_date= curdate();

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

			DROP TEMPORARY TABLE IF EXISTS tmp_all_customer;
			CREATE TEMPORARY TABLE IF NOT EXISTS tmp_all_customer ( INDEX(origination_loan_id) ) 
			AS (
			select 
			la.lms_customer_id, 
			la.lms_application_id,
			la.origination_loan_id,
			la.product,
			la.state,
			la.customer_firstname as FirstName, 
			la.customer_lastname as LastName, 
			la.pay_frequency,
			max(la.emailaddress) as Email,
			la.loan_status,
			if(la.loan_status ='Originated', 'Active', if(la.loan_status ='Paid Off', 'Inactive', '')) as Status_Group,
			date_format(la.origination_time,'%Y-%m-%d') as origination_date,
			la.approved_amount,
			(select lc.credit_limit from jaglms.loc_customer_statements lc where lc.base_loan_id=la.origination_loan_id limit 1) as original_credit_limit
			from reporting_cf.leads_accepted la 
			where la.lms_code='JAG' 
			and la.isoriginated=1
			and la.origination_time between @std_date and @end_date
			and la.product='LOC'
			and la.loan_status ='Originated'
			and la.isapplicationtest=0
			group by la.lms_customer_id
			);

			DROP TEMPORARY TABLE IF EXISTS tmp_all_customer2;
			CREATE TEMPORARY TABLE IF NOT EXISTS tmp_all_customer2 
			AS (
			select c.*,
						 sum(if(psi.total_amount<0 and psi.status ='Cleared', psi.amount_prin, 0)) as total_draw_amount,
						 sum(if(psi.total_amount<0 and psi.status ='Cleared', 1, 0)) as total_draw_count,
						 sum(if(psi.total_amount>0 and psi.status in ('Cleared', 'Correction'), psi.amount_prin, 0)) as total_prin_paid,
						 max(if(psi.total_amount<0 and psi.status ='Cleared', psi.item_date, '')) as last_draw_date,
						 max(if(psi.total_amount>0 and psi.status in ('Cleared', 'Correction'), psi.item_date, '')) as last_payment_date,
						 sum(if(psi.total_amount>0 and psi.status in ('Missed', 'Return'),1,0)) as total_default_count, 
						 sum(if(psi.total_amount>0,1,0)) as total_payment_count 
			from tmp_all_customer c
			left join reporting_cf.vcf_lms_payment_schedules ps on c.origination_loan_id=ps.base_loan_id
			left join reporting_cf.vcf_lms_payment_schedule_items psi on ps.payment_schedule_id = psi.payment_schedule_id and psi.item_date<=curdate() 
																												and psi.status in ('Missed', 'Return', 'Cleared','Correction')
			group by c.lms_customer_id);
																										
																												

			DROP TEMPORARY TABLE IF EXISTS tmp_all_customer3;
			CREATE TEMPORARY TABLE IF NOT EXISTS tmp_all_customer3 ( INDEX(lms_customer_id) ) 
			AS (
			select c.*,
						#(c.original_credit_limit+c.total_draw_amount+c.total_prin_paid) as available_credit_limit,
						(c.approved_amount+c.total_draw_amount+c.total_prin_paid) as available_credit_limit,
						datediff(curdate(), c.last_draw_date) as days_since_last_draw,
						datediff(curdate(), c.last_payment_date) as days_since_last_payment
			from tmp_all_customer2 c); 

			DROP TEMPORARY TABLE IF EXISTS tmp_exc1;
			CREATE TEMPORARY TABLE IF NOT EXISTS tmp_exc1  
			AS (
			select distinct t1.lms_customer_id
			from tmp_all_customer3 t1
			join reporting_cf.vcf_lms_customer_info_flat cf on t1.lms_customer_id = cf.customer_id 
			where cf.optout_marketing_email='true'
			);
		
			DROP TEMPORARY TABLE IF EXISTS tmp_all_list;
			CREATE TEMPORARY TABLE IF NOT EXISTS tmp_all_list 
			AS (
			select f.*,
			(f.available_credit_limit/f.approved_amount) as avail_credit_rate,
			case when f.available_credit_limit>=f.approved_amount then '100%'
					 when (f.available_credit_limit>=0.9*f.approved_amount) and (f.available_credit_limit<1*f.approved_amount)  then '90%-99%'
					 when (f.available_credit_limit>=0.8*f.approved_amount) and (f.available_credit_limit<0.9*f.approved_amount)  then '80%-89%'
					 when (f.available_credit_limit>=0.7*f.approved_amount) and (f.available_credit_limit<0.8*f.approved_amount)  then '70%-79%'
					 when (f.available_credit_limit>=0.6*f.approved_amount) and (f.available_credit_limit<0.7*f.approved_amount)  then '60%-69%'
					 when (f.available_credit_limit>=0.5*f.approved_amount) and (f.available_credit_limit<0.6*f.approved_amount)  then '50%-59%'
					 when (f.available_credit_limit>=0.4*f.approved_amount) and (f.available_credit_limit<0.5*f.approved_amount)  then '40%-49%'
					 when (f.available_credit_limit>=0.25*f.approved_amount) and (f.available_credit_limit<0.4*f.approved_amount)  then '25%-40%'
					 else '<25%'
			 end as Available_credit_range
			from tmp_all_customer3 f
			left join tmp_exc1 e1 on f.lms_customer_id=e1.lms_customer_id
			where e1.lms_customer_id is null);

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
			SET @process_label ='add data into target table', @process_type = 'Insert';	
			INSERT INTO reporting_cf.loc_gc_campaign_history
			(list_generation_date, job_id, lms_customer_id,lms_application_id, origination_loan_id, origination_date, loan_status, status_group,
			first_name,last_name, email, state, product, pay_frequency, 
			approved_amount,original_credit_limit, total_draw_amount, total_draw_count, total_prin_paid, 
			last_draw_date, last_payment_date, total_default_count, total_payment_count,
			available_credit_limit, days_since_last_draw, days_since_last_payment, available_credit_rate, available_credit_range)
			select 
			curdate() as list_generation_date,
			date_format(now(), 'LOC%m%d%YGC') as job_ID,
			lms_customer_id,lms_application_id, origination_loan_id, origination_date, loan_status, status_group,
			firstname,lastname, email, state, product, pay_frequency, 
			approved_amount,original_credit_limit, total_draw_amount, total_draw_count, total_prin_paid, 
			last_draw_date, last_payment_date, total_default_count, total_payment_count,
			available_credit_limit, days_since_last_draw, Days_since_last_payment, avail_credit_rate, Available_credit_range
			from tmp_all_list
			where days_since_last_draw>=15 
						and (avail_credit_rate> 0.4 or available_credit_limit>=100) 
						and total_default_count< 3 and total_payment_count > 3;
            
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
