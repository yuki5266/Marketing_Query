DROP PROCEDURE IF EXISTS reporting.SP_campaign_history_ACHD;
CREATE PROCEDURE reporting.`SP_campaign_history_ACHD`()
BEGIN
/*******************************************************************************************************************
---    NAME : SP_campaign_history_ACHD
---    DESCRIPTION: script for populate table  campaign_history for list module Abandon 105min_45min and Abandon Yesterday in reporting schema
---    DD/MM/YYYY    By              Comment
----   08/11/2019                    DAT-1173 initial version
----   16/12/2019  									 DAT-1273 new logic / code from marketting team
********************************************************************************************************************/
  SET SQL_SAFE_UPDATES=0;
  SET @start = 'Start', @end = 'End', @success = ' succeeded,', @failed = ' failed, returned SQL_STATE = ', @error_msg = ', error message = ', @total_rows = ' total row count = '; 
  SET @process_name = 'SP_campaign_history_ACHD', @status_flag_success = 1, @status_flag_failure = 0;
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
			select t.user_name, t.lms_customer_id, max(t.`timestamp`) as dropoff_time, count(t.id) as record_cnt, ai.firstname, ai.state, ai.amount
					from webapi.tracking t
					left join webapi.user_application_info ai on t.user_name=ai.email and ai.organization_id=1
					where t.organization_id=1 and t.lead_sequence_id is null  
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
													 where t2.page='reject' and t2.organization_id=1) e on a.user_name=e.user_name);
						 
						
														
				DROP TEMPORARY TABLE IF EXISTS table1;
						CREATE TEMPORARY TABLE IF NOT EXISTS table1 
						AS ( select a.* from all_ac a 
								 left join exc e on e.user_name=a.user_name
								 where e.user_name is null);

								 
				##exclude any accepted customer by email address
				DROP TEMPORARY TABLE IF EXISTS exc1;
				CREATE TEMPORARY TABLE IF NOT EXISTS exc1 
				AS ( select t1.*,la.lms_customer_id as lacf_customer_id,la.application_status,la.loan_status from table1 t1
						inner join reporting.leads_accepted la on t1.user_name=la.emailaddress);
			
															 
				DROP TEMPORARY TABLE IF EXISTS table2;
						CREATE TEMPORARY TABLE IF NOT EXISTS table2 
						AS ( select t1.* from table1 t1 
								 left join exc1 e on e.user_name=t1.user_name
								 where e.user_name is null
											 and t1.state in  ('TX', 'TN','NM','MS','MO','KS'));
                       
         ####another condition: exclude the accepts with different email, use customer id
        DROP TEMPORARY TABLE IF EXISTS exc2;
        CREATE TEMPORARY TABLE IF NOT EXISTS exc2 
        AS (
        select distinct table2.user_name
        from table2
        inner join webapi.tracking tr on table2.user_name=tr.user_name 
                                   and tr.lms_customer_id is not null 
                                   and tr.lms_customer_id>0 -- !=(-1)
                                   and tr.organization_id=1
                                   and tr.`timestamp`>=date_sub(curdate(), interval 2 month));

        DROP TEMPORARY TABLE IF EXISTS table3;
        CREATE TEMPORARY TABLE IF NOT EXISTS table3 
        AS ( select t2.* from table2 t2 
           left join exc2 e2 on e2.user_name=t2.user_name
           where e2.user_name is null);  
           
       ###DM flag  --- DAT-1273

				DROP TEMPORARY TABLE IF EXISTS dm_amount;
				CREATE TEMPORARY TABLE IF NOT EXISTS dm_amount ( INDEX(state_code) ) 
				AS (
				select ml2.id, ml2.state_code, ml2.loan_amount_offer, ml2.effective_date
				from(
				select ml.state_code, max(ml.id) as last_update_id
				from shared.direct_mail_lookup ml
				group by ml.state_code) mlf 
				left join shared.direct_mail_lookup ml2 on mlf.last_update_id=ml2.id);


				###when multiple emails apply with same promo code, use the first apply email
				DROP TEMPORARY TABLE IF EXISTS dm_applications;
				CREATE TEMPORARY TABLE IF NOT EXISTS dm_applications ( INDEX(state, promo_code) ) 
				AS (
				select email, state, promo_code, campaign_name, validation_date, organization_id, create_datetime, count(*) as cnt
				from webapi.direct_mail_application_log 
				where application_status=1
							and create_datetime >=date_sub(curdate(), interval 60 day)
							and email not like '%moneykey.com' -- DAT-1273
							and email not like 'test@%'
				group by promo_code);
        
        ###When overlap for same email with multiple codes, use the most recent code they used
        DROP TEMPORARY TABLE IF EXISTS last_application;
        CREATE TEMPORARY TABLE IF NOT EXISTS last_application ( INDEX(email, recent_validation_date) ) 
        AS (select email, max(validation_date) as recent_validation_date
            from dm_applications
            group by email);


        DROP TEMPORARY TABLE IF EXISTS dm_applications_2;
        CREATE TEMPORARY TABLE IF NOT EXISTS dm_applications_2 ( INDEX(state, promo_code) ) 
        AS (
        select dm.*
        from last_application aa   
        inner join dm_applications dm on aa.email=dm.email and aa.recent_validation_date=dm.validation_date
        );

        

				DROP TEMPORARY TABLE IF EXISTS dm_all;
				CREATE TEMPORARY TABLE IF NOT EXISTS dm_all ( INDEX(email) ) 
				AS (
				select dm.*, am.loan_amount_offer, map.start_date, map.expire_date
				from dm_applications_2 dm 
				inner join dm_amount am on dm.state=am.state_code
				inner join reporting.dm_campaign_mapping map on dm.promo_code like map.affid and map.expire_date>=curdate());                 
					
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
			SET @process_label ='populate data for list module ACH', @process_type = 'Insert';
			set
			@valuation_date = curdate(),
			@channel='email',
			@list_name='Abandon 105min_45min',
			@list_module='ACH',
			@list_frq='H',
			@list_gen_time= now(),
			@time_filter='dropoff_time';
			INSERT INTO reporting.campaign_history
			(business_date, Channel,       list_name,   job_ID, list_module,    list_frq,    email,  Customer_FirstName,
			Req_Loan_Amount, received_time, key_word, list_generation_time, 
      state, approved_amount, extra1, promo_code, dm_expire_date) -- DAT-1273
				select @valuation_date,
							@channel,
							@list_name as list_name,
							date_format(@list_gen_time, '%m%d%YACH') as job_ID, 
							@list_module as list_module,
							@list_frq as list_frq,
							a.user_name as email,  a.FirstName, a.amount as Requested_Loan_Amt, a.dropoff_time, a.record_cnt,
							@list_gen_time as list_generation_time,
              a.state,
              dm.loan_amount_offer,
              if(dm.promo_code is not null,1,0) as is_dm,
              dm.promo_code,
              dm.expire_date
				from table3 a
					left join dm_all dm on a.user_name=dm.email
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

	 
					set
					@valuation_date = curdate(),
					@channel='email',
					@list_name='Abandon Yesterday',
					@list_module='ACD',
					@list_frq='D',
					@list_gen_time= now(),
					@time_filter='dropoff_time';

					INSERT INTO reporting.campaign_history

					(business_date, Channel,       list_name,   job_ID, list_module,    list_frq,    email,  Customer_FirstName,
					Req_Loan_Amount, received_time, key_word, list_generation_time,
          state, approved_amount, extra1, promo_code, dm_expire_date) -- DAT-1273
					select @valuation_date,
									@channel,
									@list_name as list_name,
									date_format(@list_gen_time, '%m%d%YACD') as job_ID, 
									@list_module as list_module,
									@list_frq as list_frq,
									a.user_name as email,  a.FirstName, a.amount as Requested_Loan_Amt, a.dropoff_time, a.record_cnt,
									@list_gen_time as list_generation_time,
                  a.state,
									dm.loan_amount_offer,
									if(dm.promo_code is not null,1,0) as is_dm,
									dm.promo_code,
									dm.expire_date
						from table3 a
							left join dm_all dm on a.user_name=dm.email
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
