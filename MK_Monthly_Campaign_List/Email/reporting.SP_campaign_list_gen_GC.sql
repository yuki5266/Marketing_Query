DROP PROCEDURE IF EXISTS reporting.SP_campaign_list_gen_GC;
CREATE PROCEDURE reporting.`SP_campaign_list_gen_GC`()
BEGIN
/*********************************************************************************************************************************
---    NAME : SP_campaign_list_gen_GC
---    DESCRIPTION: script for monthly_campaign_history data population
---    DD/MM/YYYY    Ticket#        Comment
---    15/10/2019    DAT-1037 			initial version 
---    30/10/2019    DAT-1161       add new condition 
---    28/01/2020    DAT-1375 			update logic to exclue EPIC TX FP
---    05/02/2020    DAT-1409				change state filter 
---    07/02/2020    DAT-1416       reverse the change in ticket DAT-1409
************************************************************************************************************************************/
	DECLARE IsHoliday INT DEFAULT 0;
	DECLARE NotRunFlag INT DEFAULT 0;
  SET SQL_SAFE_UPDATES=0;
  SET @start = 'Start', @end = 'End', @success = ' succeeded,', @failed = ' failed, returned SQL_STATE = ', @error_msg = ', error message = ', @total_rows = ' total row count = '; 
  SET @process_name = 'SP_campaign_list_gen_GC', @status_flag_success = 1, @status_flag_failure = 0;
  SET @valuation_date = curdate(); 
	SET @MonthNumber = Month(curdate());
  SET @DayNumber = Day(curdate());
  SET @intervaldays = 30;
  
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

		SET @channel='email',
				@list_name='Monthly Good Customer',
				@list_module='GC',
				@list_frq='M',
				@list_gen_time= curdate(),
				@time_filter='Paid Off Date',
				@opt_out_YN= 1,
				@std_date= '2015-01-01', -- (select subdate(curdate(),@intervaldays)), 
				@end_date=curdate();
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
			/* GC List */
				DROP TEMPORARY TABLE IF EXISTS epic_pay;
				CREATE TEMPORARY TABLE IF NOT EXISTS epic_pay ( INDEX(lms_application_id) ) 
				AS (

				select distinct
				@channel,
				@list_name as list_name,
				if(la.lms_code='EPIC',date_format(@list_gen_time, 'EPC%m%d%YGC'), date_format(@list_gen_time, 'JAG%m%d%YGC')) as job_ID,
				@list_module as list_module,
				@list_frq as list_frq,
				la.lms_customer_id, 
				la.lms_application_id,
				la.received_time,
				la.lms_code, 
				la.state, 
				la.product, 
				la.storename,
				la.loan_sequence, 
				la.emailaddress as email,
				CONCAT(UCASE(SUBSTRING(la.customer_firstname, 1, 1)),LOWER(SUBSTRING(la.customer_firstname, 2))) as Customer_FirstName,
				CONCAT(UCASE(SUBSTRING(la.customer_lastname, 1, 1)),LOWER(SUBSTRING(la.customer_lastname, 2))) as Customer_LastName,
				max(vp.EffectiveDate) as last_payment_date,
				@list_gen_time as list_generation_time,
				@comment,
				la.pay_frequency,
				la.approved_amount,
				la.loan_status
				from reporting.leads_accepted la
				join ais.vw_loans vl on la.lms_application_id=if(vl.OriginalLoanId=0, vl.Id, vl.OriginalLoanId) and (case when vl.LoanStatus in ('DELETED', 'Voided Renewed Loan') then 1 else 0 end)=0
				join ais.vw_payments vp on vl.id=vp.loanid and vp.PaymentStatus = 'Checked' and vp.IsDebit=1
				where 
				la.lms_code ='EPIC'
				and la.loan_status in ('Paid Off Loan','Paid Off')
				and la.state in ('DE','IL','NM','TX','UT','CA', 'AL', 'MS', 'WI') -- DAT-1416
        -- and la.state in ('DE','IL','NM','TX','UT','CA', 'AL', 'MS', 'WI','MO','ID') -- DAT-1409
				and la.isoriginated=1
				and IF(@opt_out_YN=1, la.Email_MarketingOptIn=1, la.Email_MarketingOptIn IN (1, 0))
				and la.IsApplicationTest=0
        and (case when la.state='TX' and la.product='FP' then 1 else 0 end)=0 -- DAT-1375
				group by la.lms_customer_id, la.lms_application_id    
				);

				DROP TEMPORARY TABLE IF EXISTS jag_pay;
				CREATE TEMPORARY TABLE IF NOT EXISTS jag_pay ( INDEX(lms_application_id) ) 
				AS (

				select distinct
				@channel,
				@list_name as list_name,
				if(la.lms_code='EPIC',date_format(@list_gen_time, 'EPC%m%d%YGC'), date_format(@list_gen_time, 'JAG%m%d%YGC')) as job_ID,
				@list_module as list_module,
				@list_frq as list_frq,
				la.lms_customer_id, 
				la.lms_application_id,
				la.received_time,
				la.lms_code, 
				la.state, 
				la.product, 
				la.storename,
				la.loan_sequence, 
				la.emailaddress as email,
				CONCAT(UCASE(SUBSTRING(la.customer_firstname, 1, 1)),LOWER(SUBSTRING(la.customer_firstname, 2))) as Customer_FirstName,
				CONCAT(UCASE(SUBSTRING(la.customer_lastname, 1, 1)),LOWER(SUBSTRING(la.customer_lastname, 2))) as Customer_LastName,
				max(item_date) as last_payment_date,

				@list_gen_time as list_generation_time,
				@comment,
				la.pay_frequency,
				la.approved_amount,
				lbl.loan_status
				from reporting.leads_accepted la
				join jaglms.lms_base_loans lbl on la.loan_number=lbl.base_loan_id
				join jaglms.lms_payment_schedules lps on la.lms_customer_id=lps.customer_id and la.loan_number=lps.base_loan_id 
				join jaglms.lms_payment_schedule_items lpsi on lps.payment_schedule_id = lpsi.payment_schedule_id and lpsi.status='Cleared' and lpsi.total_amount>0 -- lpsi.item_type!='C'-- ='D'

				where 
				la.lms_code = 'JAG'
				and lbl.loan_status in ('Paid Off Loan','Paid Off')
				-- 'Pending Paid Off',
				#'DEFAULT-PIF')
				and la.state in ('DE','IL','NM','TX','UT','CA', 'AL', 'MS', 'WI', 'DE', 'ID', 'MO') ##OH was deleted
				and la.isoriginated=1
				and IF(@opt_out_YN=1, la.Email_MarketingOptIn=1, la.Email_MarketingOptIn IN (1, 0))
				and la.IsApplicationTest=0
				group by la.lms_customer_id, la.lms_application_id
				-- ,lps.payment_schedule_id -- if duplicate, means in collection
						 
				);

				DROP TEMPORARY TABLE IF EXISTS epic_gc;
				CREATE TEMPORARY TABLE IF NOT EXISTS epic_gc ( INDEX(lms_application_id) ) 
				AS (
				select full.*
				from epic_pay full
				where Date(full.last_payment_date) between @std_date and @end_date

				-- No additional loan or Pending Application / No Bad Previous Loan (Collection) / No Withdrawal & Pending
				and full.lms_customer_id not in
						(       select la2.lms_customer_id from reporting.leads_accepted la2 
										 where 
												(date(la2.origination_time) >=date(full.last_payment_date)           -- No additional Loan
												or (la2.application_status='Pending' and date(la2.received_time) >=date(full.last_payment_date)) -- No following Pedning Application
												or la2.loan_status in ('Returned Item','Charged Off','Default', 'DEFAULT-SLD', 'DEFAULT-BKC', 'DEFAULT-SIF','DEFAULT-FRD') -- No Previous Bad Loan
												or (la2.application_status in ('Withdrawn', 'Withdraw') and date(la2.received_time) >=date(full.last_payment_date)
														-- and la2.withdrawn_reason_code not in (1,2,10,16,19,22,23,24,25,26,27)
                            and la2.withdrawn_reason_code in (3,6,15,21,29) -- DAT-1301
                            )               -- No following Withdrawal-cannot remarket
												) and la2.lms_code='EPIC')                   
							);

				DROP TEMPORARY TABLE IF EXISTS jag_gc;
				CREATE TEMPORARY TABLE IF NOT EXISTS jag_gc ( INDEX(lms_application_id) ) 
				AS (
				select full2.*
				from jag_pay full2
				where Date(full2.last_payment_date) between @std_date and @end_date

				-- No additional loan or Pending Application / No Bad Previous Loan (Collection) / No Withdrawal & Pending
				and full2.lms_customer_id not in
						(       select la2.lms_customer_id from reporting.leads_accepted la2 
										 where 
												(date(la2.origination_time) >=date(full2.last_payment_date)           -- No additional Loan
												or (la2.application_status='Pending' and date(la2.received_time) >=date(full2.last_payment_date)) -- No following Pedning Application
												or la2.loan_status in ('Returned Item','Charged Off','Default', 'DEFAULT-SLD', 'DEFAULT-BKC', 'DEFAULT-SIF','DEFAULT-FRD') -- No Previous Bad Loan
												or (la2.application_status in ('Withdrawn', 'Withdraw') and date(la2.received_time) >=date(full2.last_payment_date)
														-- and la2.withdrawn_reason_code not in (1,2,10,16,19,22,23,24,25,26,27)
                            and la2.withdrawn_reason_code in (3,6,15,21,29) -- DAT-1301
                            )                                 -- No following Withdrawal-cannot remarket  
												 ) and la2.lms_code='JAG')
							);


				DROP TEMPORARY TABLE IF EXISTS table1;
				CREATE TEMPORARY TABLE IF NOT EXISTS table1 
				AS (
				select * from
				(select * from epic_gc
				 union
				 select * from jag_gc) c);

				DROP TEMPORARY TABLE IF EXISTS exc;
				CREATE TEMPORARY TABLE IF NOT EXISTS exc ( INDEX (email) ) 
				AS (

				select distinct t1.email, t1.received_time  -- DAT-1161
        from table1 t1 
				join reporting.leads_accepted t2 on t1.email=t2.emailaddress and t1.lms_code <>t2.lms_code 
				where t2.origination_time>=t1.last_payment_date
				or (t2.application_status = 'Pending' and t2.received_time>=t1.last_payment_date)
				);

				##ADD max_loan_limit/next_loan_limit
				DROP TEMPORARY TABLE IF EXISTS next_loan_limit;
				CREATE TEMPORARY TABLE IF NOT EXISTS next_loan_limit 
				AS (
				SELECT final.*,
							 case when final.product_limit='PD' then 255  
										 when final.PF_current='B' then least(ceiling((final.netmonthlyincome_current/2.16667)*dd.RPP/25)*25,dd.hardcap)
										 when final.PF_current='S' then least(ceiling((final.netmonthlyincome_current/2)*dd.RPP/25)*25,dd.hardcap)
										 when final.PF_current='W' then least(ceiling((final.netmonthlyincome_current/4.3333)*dd.RPP/25)*25,dd.hardcap)
										 when final.PF_current='M' then least(ceiling((final.netmonthlyincome_current/1)*dd.RPP/25)*25,dd.hardcap)
										 else null
								end as next_loan_limit,
								dd.min_amt, 
								dd.hardcap
				FROM
							(SELECT a.*, 
											if(a.loan_sequence+1>7, 7, a.loan_sequence+1) as Next_loan_sequence_limit,
											 case when a.state='TX' and a.product='IPP' and a.storename like '%BAS%' then 'IPP-BAS'
														when a.state='TX' and a.product='IPP' and a.storename like '%NCP%' then 'IPP-NCP'
														else a.product
											 end as product_limit,
											 if(a.lms_code='EPIC', vp.TotalPerPaycheck*if(Left(vp.FrequencyType,1)='B',2.16667,if(Left(vp.FrequencyType,1) ='W',4.3333, if(Left(vp.FrequencyType,1)='S',2,1))), lcif.nmi) as netmonthlyincome_current,
											 if(a.lms_code='EPIC', vp.totalperpaycheck, lcif.paycheck_amount) as Paycheck_current,
											 case when a.lms_code='EPIC' and vp.frequencytype='Bi-Weekly' then 'B'
														when a.lms_code='EPIC' and vp.frequencytype='Semi-Monthly' then 'S'
														when a.lms_code='EPIC' and vp.frequencytype='Weekly' then 'W' 
														when a.lms_code='EPIC' and vp.frequencytype='Monthly' then 'M' 
														when a.lms_code='JAG' then lcif.payfrequency
														else null
											 end as PF_current
								from table1 a
							left join ais.vw_payroll vp on a.lms_customer_id=vp.clientId and a.lms_code='EPIC'
							left join jaglms.lms_customer_info_flat lcif on a.lms_customer_id=lcif.customer_id and a.lms_code = 'JAG') final
				left join reporting.vw_loan_limit_rates dd on final.product_limit = dd.product_code and final.state = dd.state_code and  final.Next_loan_sequence_limit= dd.loan_sequence and final.PF_current = dd.pay_frequency);

				### 2019-09-05: add cell_phone column on table2
				DROP TEMPORARY TABLE IF EXISTS table2;
				CREATE TEMPORARY TABLE IF NOT EXISTS table2 
				AS (
				select t1.*, 
							 datediff(t1.list_generation_time, t1.last_payment_date) as Days_since_paid_off,
							 case when datediff(t1.list_generation_time, t1.last_payment_date) <=45 then 'GC Active'
										when datediff(t1.list_generation_time, t1.last_payment_date)>45 and datediff(t1.list_generation_time, t1.last_payment_date) <=180 then 'GC Engaged'
										when datediff(t1.list_generation_time, t1.last_payment_date) >180 then 'GC Dormant'
										else null
							 end as 'GC Group',      
							 case  when t1.state='OH' AND t1.product='SP' then 'OH_SP'   #days since paid off>=12
										 when t1.state='CA' AND t1.product='PD' then 'CA_PD'   ##days since paid off>=60
										 when t1.state='AL' AND t1.product='SEP' then 'AL_SEP' ##days since paid off>=95
										 when t1.state='CA' AND t1.product='SEP' then 'CA_SEP' ##days since paid off>=95                          
										 ELSE 'OTHERS' #others days since paid off>=80
										 END AS State_Filter,
							 IF(t1.lms_code = 'JAG',
							 (CASE
									 WHEN ff.cellphone = 9999999999 THEN ff.homephone
									 WHEN ff.cellphone = 0000000000 THEN ff.homephone
									 WHEN ff.cellphone = " " THEN ff.homephone
									 ELSE ff.cellphone
								END),
							 (CASE
									 WHEN tt.cellphone = '(999)999-9999' THEN tt.homephone
									 WHEN tt.cellphone = " " THEN tt.homephone
									 ELSE tt.cellphone
								END)) as cell_phone

				from next_loan_limit t1
				left join exc e on t1.email=e.email and e.received_time=t1.received_time -- DAT-1161        
				left JOIN jaglms.lms_customer_info_flat ff ON t1.lms_customer_id = ff.customer_id AND t1.lms_code = 'JAG'
				left JOIN ais.vw_client tt ON t1.lms_customer_id = tt.id AND t1.lms_code = 'EPIC'
				where e.email is null);

			IF sql_code = '00000' THEN
				GET DIAGNOSTICS rowCount = ROW_COUNT;
				SET return_message = CONCAT(@process_type, @success, @total_rows,rowCount);
				CALL reporting.SP_process_log(@valuation_date, @process_name, @process_label, @process_type, return_message, @status_flag_success);
			ELSE
				SET return_message = CONCAT(@process_type, @failed, sql_code, @error_msg ,sql_msg);
				CALL reporting.SP_process_log(@valuation_date, @process_name, @process_label, @process_type, return_message, @status_flag_failure);
			END IF;
		END;	
	-- ******manually check the list before inserting into reporting.monthly_campaign_history;  if there is duplicate in JAG, means the status is not correct, this case may in collection
		-- add loan sequence
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
		INSERT INTO reporting.monthly_campaign_history

		(Channel,   list_name,  job_ID,     list_module,      list_frq,   lms_customer_id,  lms_application_id, received_time,    lms_code,   state,      product,      loan_sequence,    email,      Customer_FirstName,      
		Customer_LastName,      last_repayment_date,list_generation_time, Comments, pay_frequency, approved_amount, days_since_paid_off, GC_Group, min_amt, hardcap,cell_phone,max_loan_limit)

		select @Channel, t2.list_name, t2.job_ID, t2.list_module, t2.list_frq,  
					 t2.lms_customer_id,  t2.lms_application_id, t2.received_time, t2.lms_code, t2.state,  t2.product,   t2.loan_sequence, t2.email,  t2.Customer_FirstName, t2.Customer_LastName,
					 t2.last_payment_date, t2.list_generation_time, @Comments, t2.pay_frequency,
					 t2.approved_amount, t2.days_since_paid_off, t2.`GC Group`,
					 t2.min_amt, t2.hardcap,t2.cell_phone,t2.next_loan_limit    
		from table2 t2
		where ((t2.State_Filter='CA_PD' and t2.Days_since_paid_off>=65) or (t2.State_Filter in ('AL_SEP')  -- DAT-1289
    and t2.Days_since_paid_off>=95) or (t2.State_Filter='OTHERS' and t2.Days_since_paid_off>=80))
					and t2.state!='OH';

			/*
			select list_module,  Channel,
						 lms_customer_id,  lms_application_id, received_time, lms_code, state, product, loan_sequence, email, Customer_FirstName, Customer_LastName,
						 last_repayment_date, list_generation_time, pay_frequency,
						 approved_amount, days_since_paid_off, GC_Group,cell_phone
						 min_amt, hardcap
			from reporting.monthly_campaign_history where date(list_generation_time)=curdate() and list_module='GC'; */

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
