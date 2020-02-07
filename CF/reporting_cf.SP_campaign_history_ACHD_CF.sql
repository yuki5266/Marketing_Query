DROP PROCEDURE IF EXISTS reporting_cf.SP_campaign_history_ACHD_CF;
CREATE PROCEDURE reporting_cf.`SP_campaign_history_ACHD_CF`()
BEGIN
/*******************************************************************************************************************
---    NAME : SP_campaign_history_ACHD_CF
---    DESCRIPTION: script for populate table  campaign_history for list module Abandon 105min_45min and Abandon Yesterday in reporting_cf schema
---    DD/MM/YYYY    By              Comment
---    22/07/2019    Eric Pu         DAT-948 wrape it based on Joyce's script and add exception handling and logs
																		 The list module Abandon 105min_45min will run hourly and Abandon Yesterday will
                                     run daily at 10:30 am EST
----   12/08/2019                    add logic to exclude all rejected users  
----   21/10/2019                    DAT-987  add date function
----   08/11/2019                    DAT-1173 redefine the logic
----   14/01/2020                    DAT-1310 Update reporting_cf.SP_campaign_history_ACHD_CF
----   05/02/2020                    DAT-1412 Update reporting_cf.SP_campaign_history_ACHD_CF
********************************************************************************************************************/
  SET SQL_SAFE_UPDATES=0;
  SET @start = 'Start', @end = 'End', @success = ' succeeded,', @failed = ' failed, returned SQL_STATE = ', @error_msg = ', error message = ', @total_rows = ' total row count = '; 
  SET @process_name = 'SP_campaign_history_ACHD_CF', @status_flag_success = 1, @status_flag_failure = 0;
  SET @valuation_date = curdate();  
  SELECT HOUR(CURTIME()) INTO @runhour;
  SELECT weekday(curdate()) INTO @weekday; 
  SELECT FLOOR((DAYOFMONTH(curdate()) - 1)/7 +1) INTO @weeknum;   
   
	CALL reporting.SP_process_log(@valuation_date, @process_name, @start, null, 'job is running', null);
  -- prepare temporay tables 
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
			SET @process_label ='Create temporary tables', @process_type = 'Create';
			DROP TEMPORARY TABLE IF EXISTS all_ac;
			CREATE TEMPORARY TABLE IF NOT EXISTS all_ac ( INDEX (user_name) ) 
					AS (
					select t.user_name,
					t.lms_customer_id, 
					max(t.`timestamp`) as dropoff_time,
					count(t.id) as record_cnt, 
					ai.firstname, 
					ai.state, 
					ai.amount
							from webapi.tracking t
							left join webapi.user_application_info ai on t.user_name=ai.email and ai.organization_id=2
							where t.organization_id=2 and t.lead_sequence_id is null  
								and t.user_name not like '%moneykey.com' and t.user_name not like '%creditfresh.com'
								and date(t.timestamp) >= date_sub(now(),interval 3 day)    
							group by t.lms_customer_id,  t.user_name);

				##exclude any rejected
				DROP TEMPORARY TABLE IF EXISTS exc;
						CREATE TEMPORARY TABLE IF NOT EXISTS exc ( INDEX (user_name) ) 
						AS (
						select a.*
						from all_ac a
						inner join (select distinct t2.user_name from webapi.tracking t2 
													 where t2.page='reject' and t2.organization_id=2) e on a.user_name=e.user_name);
		-- dat-1173               
		DROP TEMPORARY TABLE IF EXISTS table1;
						CREATE TEMPORARY TABLE IF NOT EXISTS table1 
						AS ( select a.* 
            from all_ac a 
						left join exc e on e.user_name=a.user_name
						 where e.user_name is null);
         
    DROP TEMPORARY TABLE IF EXISTS exc1;
				CREATE TEMPORARY TABLE IF NOT EXISTS exc1 
				AS ( select 
        t1.*,
        la.lms_customer_id as lacf_customer_id,
        la.application_status,
        la.loan_status 
        from table1 t1
				inner join reporting_cf.leads_accepted la on t1.user_name=la.emailaddress);	
        
			DROP TEMPORARY TABLE IF EXISTS table2;
						CREATE TEMPORARY TABLE IF NOT EXISTS table2 
						AS ( select t1.* 
            from table1 t1 
								 left join exc1 e on e.user_name=t1.user_name
								 where e.user_name is null
                       );
		  DROP TEMPORARY TABLE IF EXISTS exc2;
        CREATE TEMPORARY TABLE IF NOT EXISTS exc2 
        AS (
        select distinct table2.user_name
        from table2
        inner join webapi.tracking tr on table2.user_name=tr.user_name 
                                   and tr.lms_customer_id is not null 
                                   and tr.lms_customer_id>0 -- !=(-1)
                                   and tr.organization_id=2
                                   and tr.`timestamp`>=date_sub(curdate(), interval 2 month));
			DROP TEMPORARY TABLE IF EXISTS table3;
        CREATE TEMPORARY TABLE IF NOT EXISTS table3 
        AS ( select t2.* 
        from table2 t2 
           left join exc2 e2 on e2.user_name=t2.user_name
           where e2.user_name is null); 
			IF sql_code = '00000' THEN
				GET DIAGNOSTICS rowCount = ROW_COUNT;
				SET return_message = CONCAT(@process_type, @success, @total_rows,rowCount);
				CALL reporting.SP_process_log(@valuation_date, @process_name, @process_label, @process_type, return_message, @status_flag_success);
			ELSE
				SET return_message = CONCAT(@process_type, @failed, sql_code, @error_msg ,sql_msg);
				CALL reporting.SP_process_log(@valuation_date, @process_name, @process_label, @process_type, return_message, @status_flag_failure);
			END IF;
		
		END; 
  set
    @valuation_date = curdate(),
		@channel='email',
		@list_name='Abandon 105min_45min',
		@list_module='ACH',
		@list_frq='H',
		@list_gen_time= now(),
		@time_filter='dropoff_time';
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
			SET @process_label ='populate data for list module ACH', @process_type = 'Insert';
			INSERT INTO reporting_cf.campaign_history
			(business_date, Channel,       list_name,   job_ID, list_module,    list_frq,    email,  Customer_FirstName,
			Req_Loan_Amount, received_time, key_word, list_generation_time, 
      state) 
				select @valuation_date,
							@channel,
							@list_name as list_name,
							date_format(@list_gen_time, '%m%d%YACH') as job_ID, 
							@list_module as list_module,
							@list_frq as list_frq,
							a.user_name as email,  a.FirstName, a.amount as Requested_Loan_Amt, a.dropoff_time, a.record_cnt,
							@list_gen_time as list_generation_time,
              a.state
				from table3 a
					where a.dropoff_time between date_sub(now(), interval 105 minute) and date_sub(now(), interval 45 minute); 
						
			IF sql_code = '00000' THEN
				GET DIAGNOSTICS rowCount = ROW_COUNT;
				SET return_message = CONCAT(@process_type, @success, @total_rows,rowCount);
				CALL reporting.SP_process_log(@valuation_date, @process_name, @process_label, @process_type, return_message, @status_flag_success);
			ELSE
				SET return_message = CONCAT(@process_type, @failed, sql_code, @error_msg ,sql_msg);
				CALL reporting.SP_process_log(@valuation_date, @process_name, @process_label, @process_type, return_message, @status_flag_failure);
			END IF;
		
		END; 
    
		IF @runhour = 10 THEN -- only run around 10:30 everyday 	 
			set
			@valuation_date = curdate(),
			@channel='email',
			@list_name='Abandon Yesterday',
			@list_module='ACD',
			@list_frq='D',
			@list_gen_time= now(),
			@time_filter='dropoff_time';
			
				-- select @valuation_date, @channel,@list_name,@list_module,@list_frq,@list_gen_time,@time_filter,@opt_out_YN, @first_interval,@second_interval,@third_interval,@first_date,@second_date,@third_date, @comment;
	 
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
				SET @process_label ='populate data for list module ACD', @process_type = 'Insert';
				INSERT INTO reporting_cf.campaign_history
					(business_date, Channel,       list_name,   job_ID, list_module,    list_frq,    email,  Customer_FirstName,
					Req_Loan_Amount, received_time, key_word, list_generation_time,
          state) 
					select @valuation_date,
									@channel,
									@list_name as list_name,
									date_format(@list_gen_time, '%m%d%YACD') as job_ID, 
									@list_module as list_module,
									@list_frq as list_frq,
									a.user_name as email,  a.FirstName, a.amount as Requested_Loan_Amt, a.dropoff_time, a.record_cnt,
									@list_gen_time as list_generation_time,
                  a.state
						from table3 a
								where a.dropoff_time between date_sub(curdate(), interval 1 day) and curdate();
      
							
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
    
		-- log the process for completion
		CALL reporting.SP_process_log(@valuation_date, @process_name, @end, null, 'job is done', @status_flag_success);

END;
