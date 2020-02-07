		
     ############POL: EPIC TX FP
      
      
      select distinct
      CURDATE() AS Business_date,
			'Email' as Channel,
			'EPIC TX FP' as list_name,
			'POL_NEW' as list_module,
			la.lms_customer_id,
			la.lms_application_id,
			la.received_time,
			la.lms_code,
			la.state,
			la.product,
			la.loan_sequence,
			la.emailaddress as email,
      la.pay_frequency,
      if(la.pay_frequency='M',1,0) as Is_Monthly,
			CONCAT(UCASE(SUBSTRING(la.customer_firstname, 1, 1)),LOWER(SUBSTRING(la.customer_firstname, 2))) as Customer_FirstName,
			CONCAT(UCASE(SUBSTRING(la.customer_lastname, 1, 1)),LOWER(SUBSTRING(la.customer_lastname, 2))) as Customer_LastName,
			max(p.effectivedate) as last_repayment_date,
			curdate() as list_generation_time
			from reporting.leads_accepted la
			join ais.vw_loans l on la.lms_application_id=if(l.originalloanid=0, l.id, originalloanid)
			inner join ais.vw_payments p on l.id=p.LoanId
			where
			la.lms_code='EPIC'
			and l.loanstatus='Paid Off Loan'
      ##### POL_new Joyce
			and  p.EffectiveDate>=Date_sub(curdate(), interval 91 day)
			and la.state !='OH' -- DAT-792
			and p.PaymentStatus = 'Checked' 
			and p.IsDebit=1
			and IF(1=1, la.Email_MarketingOptIn=1, la.Email_MarketingOptIn IN (1, 0))


			and la.lms_customer_id not in
					(       select la2.lms_customer_id from reporting.leads_accepted la2
									 where date(la2.origination_time) >=date(la.last_paymentdate)           
											or (la2.application_status='Pending' and date(la2.received_time) >=date(la.last_paymentdate)) ##Joyce 
											or la2.loan_status in ('Returned Item Pending Paid Off','Charged Off Pending Paid Off','Returned Item','Charged Off') 
											or (la2.application_status in ('Withdrawn', 'Withdraw') and date(la2.received_time) >=date(la.last_paymentdate)
													and la2.withdrawn_reason_code in (3,6,15,21,29))  )                                
			and SUBSTR(SUBSTR(la.emailaddress, INSTR(la.emailaddress, '@'), INSTR(la.emailaddress, '.')), 2) not in ('epic.lmsmail.com', 'moneykey.com')
      and la.IsApplicationTest = 0 
      and la.product != 'LOC' -- DAT-1125
      group by la.lms_customer_id,la.lms_application_id;  
      
      
      
      
      
      ###########GC: Epic TX FP
      
  SET @valuation_date = curdate(); 
	SET @MonthNumber = Month(curdate());
  SET @DayNumber = Day(curdate());
  SET @intervaldays = 30;
 

		SET @channel='email',
				@list_name='Monthly Good Customer',
				@list_module='GC',
				@list_frq='M',
				@list_gen_time= curdate(),
				@time_filter='Paid Off Date',
				@opt_out_YN= 1,
				@std_date= '2015-01-01', -- (select subdate(curdate(),@intervaldays)), 
				@end_date=curdate();
		
			SET @process_label ='Prepare data in temporary tables', @process_type = 'Insert';	

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
				and la.state ='TX'
				and la.isoriginated=1
				and IF(@opt_out_YN=1, la.Email_MarketingOptIn=1, la.Email_MarketingOptIn IN (1, 0))
				and la.IsApplicationTest=0
        AND (case when  la.state='TX' and la.product='FP' then 1 else 0 end)=1
				group by la.lms_customer_id, la.lms_application_id    
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

				

			

				DROP TEMPORARY TABLE IF EXISTS exc;
				CREATE TEMPORARY TABLE IF NOT EXISTS exc ( INDEX (email) ) 
				AS (

				select distinct t1.email, t1.received_time  -- DAT-1161
        from epic_gc t1 
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
								from epic_gc a
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

		/*INSERT INTO reporting.monthly_campaign_history

		(Channel,   list_name,  job_ID,     list_module,      list_frq,   lms_customer_id,  lms_application_id, received_time,    lms_code,   state,      product,      loan_sequence,    email,      Customer_FirstName,      
		Customer_LastName,      last_repayment_date,list_generation_time, Comments, pay_frequency, approved_amount, days_since_paid_off, GC_Group, min_amt, hardcap,cell_phone,max_loan_limit)*/

		select @Channel, t2.list_name, t2.job_ID, t2.list_module, t2.list_frq,  
					 t2.lms_customer_id,  t2.lms_application_id, t2.received_time, t2.lms_code, t2.state,  t2.product,   t2.loan_sequence, t2.email,  t2.Customer_FirstName, t2.Customer_LastName,
					 t2.last_payment_date, t2.list_generation_time, @Comments, t2.pay_frequency,
					 t2.approved_amount, t2.days_since_paid_off, t2.`GC Group`,
					 t2.min_amt, t2.hardcap,t2.cell_phone,t2.next_loan_limit    
		from table2 t2
		where ((t2.State_Filter='CA_PD' and t2.Days_since_paid_off>=65) or (t2.State_Filter in ('AL_SEP')  -- DAT-1289
    and t2.Days_since_paid_off>=95) or (t2.State_Filter='OTHERS' and t2.Days_since_paid_off>=80))
					and t2.state!='OH';




---------------------------- select * from temp.yyy_TX_PF_POL_GC
CALL temp.SP_EPIC_TX_FP_POL_N;

DROP PROCEDURE IF EXISTS temp.SP_EPIC_TX_FP_POL_N;
CREATE PROCEDURE temp.SP_EPIC_TX_FP_POL_N()

BEGIN

INSERT INTO temp.yyy_TX_PF_POL_GC  	
(business_date, Channel, list_name, 
list_module,  lms_customer_id, lms_application_id, received_time, lms_code,
state, product, loan_sequence, email, pay_frequency,Is_Monthly,Customer_FirstName, 
      	Customer_LastName, last_repayment_date,list_generation_time)   

      select distinct
      CURDATE() AS Business_date,
			'Email' as Channel,
			'EPIC TX FP' as list_name,
			'POL_NEW' as list_module,
			la.lms_customer_id,
			la.lms_application_id,
			la.received_time,
			la.lms_code,
			la.state,
			la.product,
			la.loan_sequence,
			la.emailaddress as email,
      la.pay_frequency,
      if(la.pay_frequency='M',1,0) as Is_Monthly,
			CONCAT(UCASE(SUBSTRING(la.customer_firstname, 1, 1)),LOWER(SUBSTRING(la.customer_firstname, 2))) as Customer_FirstName,
			CONCAT(UCASE(SUBSTRING(la.customer_lastname, 1, 1)),LOWER(SUBSTRING(la.customer_lastname, 2))) as Customer_LastName,
			max(p.effectivedate) as last_repayment_date,
			curdate() as list_generation_time
			from reporting.leads_accepted la
			join ais.vw_loans l on la.lms_application_id=if(l.originalloanid=0, l.id, originalloanid)
			inner join ais.vw_payments p on l.id=p.LoanId
			where
			la.lms_code='EPIC'
			and l.loanstatus='Paid Off Loan'
      ##### POL_new Joyce
			-- and  p.EffectiveDate>=Date_sub(curdate(), interval 91 day)
       and  p.EffectiveDate between  Date_sub(curdate(), interval 91 day) and Date_sub(curdate(), interval 3 day)
			and la.state !='OH' -- DAT-792
			and p.PaymentStatus = 'Checked' 
			and p.IsDebit=1
			and IF(1=1, la.Email_MarketingOptIn=1, la.Email_MarketingOptIn IN (1, 0))


			and la.lms_customer_id not in
					(       select la2.lms_customer_id from reporting.leads_accepted la2
									 where date(la2.origination_time) >=date(la.last_paymentdate)           
											or (la2.application_status='Pending' and date(la2.received_time) >=date(la.last_paymentdate)) ##Joyce 
											or la2.loan_status in ('Returned Item Pending Paid Off','Charged Off Pending Paid Off','Returned Item','Charged Off') 
											or (la2.application_status in ('Withdrawn', 'Withdraw') and date(la2.received_time) >=date(la.last_paymentdate)
													and la2.withdrawn_reason_code in (3,6,15,21,29))  )                                
			and SUBSTR(SUBSTR(la.emailaddress, INSTR(la.emailaddress, '@'), INSTR(la.emailaddress, '.')), 2) not in ('epic.lmsmail.com', 'moneykey.com')
      and la.IsApplicationTest = 0 
      and la.product != 'LOC' -- DAT-1125
      group by la.lms_customer_id,la.lms_application_id;  

end;










call temp.SP_EPIC_TX_FP_GC;

DROP PROCEDURE IF EXISTS temp.SP_EPIC_TX_FP_GC;
CREATE PROCEDURE temp.SP_EPIC_TX_FP_GC()

BEGIN
   
  SET @valuation_date = curdate(); 
	SET @MonthNumber = Month(curdate());
  SET @DayNumber = Day(curdate());
  SET @intervaldays = 30;
 

		SET @channel='email',
				@list_name='Monthly Good Customer',
				@list_module='Epic_TX',
				@list_frq='M',
				@list_gen_time= curdate(),
				@time_filter='Paid Off Date',
				@opt_out_YN= 1,
				@std_date= '2014-01-01', -- (select subdate(curdate(),@intervaldays)), 
				@end_date=curdate();


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
         if(la.pay_frequency='M',1,0) as Is_Monthly,
				la.approved_amount,
				la.loan_status
				from reporting.leads_accepted la
				join ais.vw_loans vl on la.lms_application_id=if(vl.OriginalLoanId=0, vl.Id, vl.OriginalLoanId) and (case when vl.LoanStatus in ('DELETED', 'Voided Renewed Loan') then 1 else 0 end)=0
				join ais.vw_payments vp on vl.id=vp.loanid and vp.PaymentStatus = 'Checked' and vp.IsDebit=1
				where 
				la.lms_code ='EPIC'
				and la.loan_status in ('Paid Off Loan','Paid Off')
				and la.state ='TX'
				and la.isoriginated=1
				and IF(@opt_out_YN=1, la.Email_MarketingOptIn=1, la.Email_MarketingOptIn IN (1, 0))
				and la.IsApplicationTest=0
        AND (case when  la.state='TX' and la.product='FP' then 1 else 0 end)=1
				group by la.lms_customer_id, la.lms_application_id    
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

				

			

				DROP TEMPORARY TABLE IF EXISTS exc;
				CREATE TEMPORARY TABLE IF NOT EXISTS exc ( INDEX (email) ) 
				AS (

				select distinct t1.email, t1.received_time  -- DAT-1161
        from epic_gc t1 
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
								from epic_gc a
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

		INSERT INTO temp.yyy_TX_PF_POL_GC

		(Business_date,Channel,   list_name,      list_module,        lms_customer_id,  lms_application_id, 
    received_time,    lms_code,   state,      product,      loan_sequence,    email, pay_frequency, Is_Monthly,     Customer_FirstName,      
		Customer_LastName,      last_repayment_date,list_generation_time)

		select curdate(),@Channel, t2.list_name, t2.list_module, 
					 t2.lms_customer_id,  t2.lms_application_id, t2.received_time, t2.lms_code, t2.state,  t2.product,  
           t2.loan_sequence, t2.email, t2.pay_frequency,t2.Is_Monthly, t2.Customer_FirstName, t2.Customer_LastName,
					 t2.last_payment_date, t2.list_generation_time
					 
		from table2 t2
		where t2.State_Filter='OTHERS' and t2.Days_since_paid_off>=91;
					


end;

-- EPIC TX FP : POL_N
CALL temp.SP_EPIC_TX_FP_POL_N;
select * from temp.yyy_TX_PF_POL_GC where Business_date=curdate() and list_module='POL_NEW';

-- EPIC TX FP : GC
call temp.SP_EPIC_TX_FP_GC;
select * from temp.yyy_TX_PF_POL_GC where Business_date=curdate() and list_module='Epic_TX';


DELETE from temp.yyy_TX_PF_POL_GC where Business_date=curdate() and list_module='Epic_TX';
