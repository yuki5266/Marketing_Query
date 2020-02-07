DROP PROCEDURE IF EXISTS reporting.SP_campaign_list_gen_OHGC;
CREATE PROCEDURE reporting.`SP_campaign_list_gen_OHGC`()
BEGIN
/*********************************************************************************************************************************
---    NAME : SP_campaign_list_gen_OHGC
---    DESCRIPTION: script for monthly_campaign_history data population
---    DD/MM/YYYY    Ticket#        Comment
---    15/10/2019    DAT-1037 			initial version 
---    30/10/2019    DAT-1161       add new condition 
************************************************************************************************************************************/
	DECLARE IsHoliday INT DEFAULT 0;
	DECLARE NotRunFlag INT DEFAULT 0;
  SET SQL_SAFE_UPDATES=0;
  SET @start = 'Start', @end = 'End', @success = ' succeeded,', @failed = ' failed, returned SQL_STATE = ', @error_msg = ', error message = ', @total_rows = ' total row count = '; 
  SET @process_name = 'SP_campaign_list_gen_OHGC', @status_flag_success = 1, @status_flag_failure = 0;
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

	IF NotRunFlag = 0 THEN*/
		-- log the start info
		CALL reporting.SP_process_log(@valuation_date, @process_name, @start, null, 'job is running', null);

		SET @channel='email',
				@list_name='GC_OH', 
				@list_module='GC_OH',
				@list_frq='W',
				@list_gen_time= now(),
				@time_filter='Paid Off Date',
				@opt_out_YN= 1,
				@std_date='2015-01-01', 
				@end_date=curdate(),
				@commet='JAG OH GC paidoff>3, since Jan012015';
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
				and la.state = 'OH'
				and la.isoriginated=1
				and IF(@opt_out_YN=1, la.Email_MarketingOptIn=1, la.Email_MarketingOptIn IN (1, 0))
				and la.IsApplicationTest=0
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
				and la.state = 'OH'
				and la.isoriginated=1
				and IF(@opt_out_YN=1, la.Email_MarketingOptIn=1, la.Email_MarketingOptIn IN (1, 0))
				and la.IsApplicationTest=0
				group by la.lms_customer_id, la.lms_application_id  
				);

				DROP TEMPORARY TABLE IF EXISTS epic_gc;
				CREATE TEMPORARY TABLE IF NOT EXISTS epic_gc ( INDEX(lms_application_id) ) 
				AS (
				select full.*
				from epic_pay full
				where Date(full.last_payment_date) between @std_date and @end_date
				and full.lms_customer_id not in
						(       select la2.lms_customer_id from reporting.leads_accepted la2 
										 where 
												(date(la2.origination_time) >=date(full.last_payment_date)           -- No additional Loan
												or (la2.application_status='Pending' and date(la2.received_time) >=date(full.last_payment_date)) -- No following Pedning Application
												or la2.loan_status in ('Returned Item','Charged Off','Default', 'DEFAULT-SLD', 'DEFAULT-BKC', 'DEFAULT-SIF','DEFAULT-FRD') -- No Previous Bad Loan
												or (la2.application_status in ('Withdrawn', 'Withdraw') and date(la2.received_time) >=date(full.last_payment_date)
														and la2.withdrawn_reason_code not in (1,2,10,16,19,22,23,24,25,26,27))               -- No following Withdrawal-cannot remarket
												) and la2.lms_code='EPIC')                   
							);
							
				 -- select * from epic_pay;
				 -- select * from epic_gc;

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
														and la2.withdrawn_reason_code not in (1,2,10,16,19,22,23,24,25,26,27))                                 -- No following Withdrawal-cannot remarket  
												 ) and la2.lms_code='JAG')
							);


				DROP TEMPORARY TABLE IF EXISTS table1;
				CREATE TEMPORARY TABLE IF NOT EXISTS table1 
				AS (
				select * from
				(select * from epic_gc
				 union
				 select * from jag_gc) c);

				select* from table1;

				DROP TEMPORARY TABLE IF EXISTS exc;
				CREATE TEMPORARY TABLE IF NOT EXISTS exc ( INDEX (email) ) 
				AS (

				select distinct t1.email, t1.received_time /*DAT-1161*/ from table1 t1 
				join reporting.leads_accepted t2 on t1.email=t2.emailaddress and t1.lms_code <>t2.lms_code 
				where t2.origination_time>=t1.last_payment_date
				or (t2.application_status = 'Pending' and t2.received_time>=t1.last_payment_date)
				);


				-- SELECT *FROM exc;

				####Exclude GC who have already applied and accepted on CF
				DROP TEMPORARY TABLE IF EXISTS exc1;
				CREATE TEMPORARY TABLE IF NOT EXISTS exc1 ( INDEX (email) ) 
				AS (
				select distinct t1.email from table1 t1 
				join reporting_cf.leads_accepted lacf on t1.email=lacf.emailaddress
				);

				### 2019-09-05: add cell_phone column on table2
				DROP TEMPORARY TABLE IF EXISTS table2;
				CREATE TEMPORARY TABLE IF NOT EXISTS table2 
				AS (
				select t1.*, 
							 datediff(t1.list_generation_time, t1.last_payment_date) as Days_since_paid_off,
							 -- '' as `GC Group`,
							 case when datediff(t1.list_generation_time, t1.last_payment_date) <=45 then 'GC Active'
										when datediff(t1.list_generation_time, t1.last_payment_date)>45 and datediff(t1.list_generation_time, t1.last_payment_date) <=180 then 'GC Engaged'
										when datediff(t1.list_generation_time, t1.last_payment_date) >180 then 'GC Dormant'
										else null
							 end as 'GC Group',    
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

				from table1 t1
				left join exc e on t1.email=e.email and e.received_time=t1.received_time -- DAT-1161
				left JOIN jaglms.lms_customer_info_flat ff ON t1.lms_customer_id = ff.customer_id AND t1.lms_code = 'JAG'
				left JOIN ais.vw_client tt ON t1.lms_customer_id = tt.id AND t1.lms_code = 'EPIC'
				where e.email is null);

				DROP TEMPORARY TABLE IF EXISTS table4;
				CREATE TEMPORARY TABLE IF NOT EXISTS table4 
				AS (
				select t2.*,
				if(marketing.`Transactional With Consent`=1 and  marketing.`Transactional Text Stop`=0,1,0) as Is_Transactional_optin,
				if(marketing.`SMS Marketing With Consent`=1 and  marketing.`SMS Marketing Text Stop`=0,1,0) as Is_SMS_Marketing_optin
				from table2 t2
				left join exc1 e1 on e1.email=t2.email         
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
								group by list.customer_id) marketing  
				on t2.lms_customer_id=marketing.customer_id and t2.lms_code='JAG'
				where e1.email is null);

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
		(Channel,   list_name,  job_ID,     list_module,      list_frq,   lms_customer_id,  lms_application_id, received_time,    lms_code,   state,     
		product,      loan_sequence,    email,      Customer_FirstName,      
		Customer_LastName,      last_repayment_date,list_generation_time,
		Comments, pay_frequency, approved_amount, days_since_paid_off, GC_Group,
		cell_phone, is_transactional_optin, is_sms_marketing_optin)
		select @Channel, t4.list_name, t4.job_ID, t4.list_module, t4.list_frq,  
					 t4.lms_customer_id,  t4.lms_application_id, t4.received_time, t4.lms_code, t4.state,  
					 t4.product,   t4.loan_sequence, t4.email,  t4.Customer_FirstName, t4.Customer_LastName,
					 t4.last_payment_date, t4.list_generation_time, @Comments, t4.pay_frequency,
					 t4.approved_amount, t4.days_since_paid_off, t4.`GC Group`,
					 t4.cell_phone,
					 Is_Transactional_optin,Is_SMS_Marketing_optin
		from table4 t4
		where t4.state='OH' and t4.Days_since_paid_off>3;

			/*select list_module,  Channel,
       lms_customer_id,  lms_application_id, received_time, lms_code, state, product, loan_sequence, email, Customer_FirstName, Customer_LastName,
       last_repayment_date, list_generation_time, pay_frequency,
       approved_amount, days_since_paid_off, GC_Group,cell_phone,
       min_amt, hardcap,is_transactional_optin, is_sms_marketing_optin
				from reporting.monthly_campaign_history where date(list_generation_time)=curdate() and list_module='GC_OH'; */

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
