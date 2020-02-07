
  SET SQL_SAFE_UPDATES=0;
  SET SESSION tx_isolation='READ-COMMITTED';
  SET @start = 'Start', @end = 'End', @success = ' succeeded,', @failed = ' failed, returned SQL_STATE = ', @error_msg = ', error message = ', @total_rows = ' total row count = '; 
  SET @process_name = 'SP_campaign_list_gen_POL_N', @status_flag_success = 1, @status_flag_failure = 0;
  SET @valuation_date = curdate(); -- may use business date in the future
  SET @MonthNumber = Month(curdate());
  SET @DayNumber = Day(curdate());
  

		set
		@channel='email',
		@list_name='Daily POL',
		@list_module='POL_NEW',
		@list_frq='D',
		@list_gen_time= now(),
		@time_filter='Paid Off Date',
		@opt_out_YN= 1,
		@test_job_id = 'JAG_TEST_POLJ';  
    

			INSERT INTO reporting.campaign_history

			(business_date, Channel,       list_name,      job_ID, list_module,    list_frq,       lms_customer_id,        lms_application_id, received_time,      lms_code,       state,  product,        loan_sequence,  email,  Customer_FirstName,
			Customer_LastName,      last_repayment_date,list_generation_time)

			select distinct
      @valuation_date,
			@channel,
			@list_name as list_name,
      '' as job_ID, ###Joyce
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
			p.effectivedate as last_repayment_date,
			@list_gen_time as list_generation_time
			from reporting.leads_accepted la
			join ais.vw_loans l on la.lms_application_id=if(l.originalloanid=0, l.id, originalloanid)
			inner join ais.vw_payments p on l.id=p.LoanId
			where
			la.lms_code='EPIC'
			and l.loanstatus='Paid Off Loan'
      ##### POL_new Joyce
			and  p.EffectiveDate>=Date_sub(@list_gen_time, interval 91 day)
			and la.state !='OH' -- DAT-792
			and p.PaymentStatus = 'Checked' 
			and p.IsDebit=1
			and IF(@opt_out_YN=1, la.Email_MarketingOptIn=1, la.Email_MarketingOptIn IN (1, 0))


			and la.lms_customer_id not in
					(       select la2.lms_customer_id from reporting.leads_accepted la2
									 where la2.lms_code='EPIC' 
                   and (date(la2.origination_time) >=date(la.last_paymentdate)           
											or (la2.application_status='Pending' and date(la2.received_time) >=date(la.last_paymentdate)) ##Joyce 
											or la2.loan_status in ('Returned Item Pending Paid Off','Charged Off Pending Paid Off','Returned Item','Charged Off') 
											or (la2.application_status in ('Withdrawn', 'Withdraw') and date(la2.received_time) >=date(la.last_paymentdate)
													and la2.withdrawn_reason_code not in (1,2,10,16,19,22,23,24,25,26,27)) ) )                                
			and SUBSTR(SUBSTR(la.emailaddress, INSTR(la.emailaddress, '@'), INSTR(la.emailaddress, '.')), 2) not in ('epic.lmsmail.com', 'moneykey.com')
      and la.IsApplicationTest = 0 
      and la.product != 'LOC';  -- DAT-1125



			INSERT INTO reporting.campaign_history

			(business_date, Channel,       list_name,      job_ID, list_module,    list_frq,       lms_customer_id,        lms_application_id, received_time,      lms_code,       state,  product,        loan_sequence,  email,  Customer_FirstName,
			Customer_LastName,      last_repayment_date,list_generation_time)
			select distinct
      @valuation_date,
			@channel,
			@list_name as list_name,
      '' as job_ID, ###Joyce
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
			b.paid_off_date as last_repayment_date,
			@list_gen_time as list_generation_time


			from reporting.leads_accepted la
			inner join jaglms.lms_base_loans b on la.lms_application_id =b.loan_header_id

			where
			la.lms_code='JAG'
			and la.state not in ('LA', 'MO','SD', 'OH') -- DAT-792
			and b.loan_status='Paid Off'
			and date(if(weekday(b.paid_off_date) in (5,6), (select pre_target_date from reporting.vw_DDR_ach_date_matching where Ori_date=b.paid_off_date), b.paid_off_date))>=Date_sub(@list_gen_time, interval 91 day)
			and IF(@opt_out_YN=1, la.Email_MarketingOptIn=1, la.Email_MarketingOptIn IN (1, 0))
			and (SELECT sum(principal_amount*-1) FROM jaglms.lms_client_transactions lct where lct.base_loan_id=b.base_loan_id) = 0
			and la.lms_customer_id not in
					(       select la2.lms_customer_id from reporting.leads_accepted la2
									 where la2.lms_code='JAG' 
                   and (date(la2.origination_time) >=date(la.last_paymentdate)           
											or (la2.application_status='Pending' and date(la2.received_time) >=date(la.last_paymentdate) ) ###Joyce
											or la2.loan_status in ('Returned Item Pending Paid Off','Charged Off Pending Paid Off','Returned Item','Charged Off', 'Charged Off Paid Off') 
											or (la2.application_status in ('Withdrawn', 'Withdraw') and date(la2.received_time) >=date(la.last_paymentdate)
													and la2.withdrawn_reason_code not in (1,2,10,16,19,22,23,24,25,26,27)))  )                                
			and SUBSTR(SUBSTR(la.emailaddress, INSTR(la.emailaddress, '@'), INSTR(la.emailaddress, '.')), 2) not in ('epic.lmsmail.com', 'moneykey.com')
      and la.product != 'LOC'; -- DAT-1125


      INSERT INTO reporting.campaign_history
      	(business_date, Channel, list_name, job_ID, list_module, list_frq, lms_customer_id, lms_application_id, received_time, lms_code, state, product, loan_sequence, email, Customer_FirstName, 
      	Customer_LastName, Req_Loan_Amount, origination_loan_id, origination_time,approved_amount,list_generation_time, Comments)      	 
        SELECT @valuation_date, @channel, @list_name, @test_job_id, @list_module, @list_frq, -9, -9, null, 'test', 'test', 'test', -9, 
          email_address, first_name, last_name, request_loan_amount, -9, null, approved_amount, @list_gen_time, comments
          FROM reporting.campaign_list_test_email
          WHERE is_active = 1;


