

  	
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
          
-- select * from all_ac;

				##exclude any rejected
				DROP TEMPORARY TABLE IF EXISTS exc;
						CREATE TEMPORARY TABLE IF NOT EXISTS exc ( INDEX (user_name) ) 
						AS (
						select a.*
						from all_ac a
						inner join (select distinct t2.user_name from webapi.tracking t2 
													 where t2.page='reject' and t2.organization_id=2) e on a.user_name=e.user_name);
						 
-- select * from exc;						
														
			DROP TEMPORARY TABLE IF EXISTS table1;
						CREATE TEMPORARY TABLE IF NOT EXISTS table1 
						AS ( select a.* 
            from all_ac a 
						left join exc e on e.user_name=a.user_name
						 where e.user_name is null);
-- select * from table1;
								 
				##exclude any accepted customer by email address
				DROP TEMPORARY TABLE IF EXISTS exc1;
				CREATE TEMPORARY TABLE IF NOT EXISTS exc1 
				AS ( select 
        t1.*,
        la.lms_customer_id as lacf_customer_id,
        la.application_status,
        la.loan_status 
        from table1 t1
				inner join reporting_cf.leads_accepted la on t1.user_name=la.emailaddress);
-- select *from exc1;			
															 
				DROP TEMPORARY TABLE IF EXISTS table2;
						CREATE TEMPORARY TABLE IF NOT EXISTS table2 
						AS ( select t1.* 
            from table1 t1 
								 left join exc1 e on e.user_name=t1.user_name
								 where e.user_name is null
                       );
   -- select * from table2;     
   
         ####another condition: exclude the accepts with different email, use customer id
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

-- select * from exc2;

        DROP TEMPORARY TABLE IF EXISTS table3;
        CREATE TEMPORARY TABLE IF NOT EXISTS table3 
        AS ( select t2.* 
        from table2 t2 
           left join exc2 e2 on e2.user_name=t2.user_name
           where e2.user_name is null);  
-- select * from table3;           
       
	
			SET @process_label ='populate data for list module ACH', @process_type = 'Insert';
      
			set
			@valuation_date = curdate(),
			@channel='email',
			@list_name='Abandon 105min_45min',
			@list_module='ACH',
			@list_frq='H',
			@list_gen_time= now(),
			@time_filter='dropoff_time';
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
	
  
  
  
				SET @process_label ='populate data for list module ACD', @process_type = 'Insert';

	 
					set
					@valuation_date = curdate(),
					@channel='email',
					@list_name='Abandon Yesterday',
					@list_module='ACD',
					@list_frq='D',
					@list_gen_time= now(),
					@time_filter='dropoff_time';

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
				