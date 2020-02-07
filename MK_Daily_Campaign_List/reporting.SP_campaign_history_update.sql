DROP PROCEDURE IF EXISTS reporting.SP_campaign_history_update;
CREATE PROCEDURE reporting.`SP_campaign_history_update`()
BEGIN
/*********************************************************************************************************************************
---    NAME : SP_campaign_history_update
---    DESCRIPTION: script for updating the campaign_history after the list data get generated daily
---    DD/MM/YYYY    By              Comment
---    08/04/2019    Eric Pu         DAT-768 Add SMS Flag inro campaign history
---																	 DAT-770 Add Phone Information into campaign history
************************************************************************************************************************************/
	DECLARE IsHoliday INT DEFAULT 0;
	DECLARE NotRunFlag INT DEFAULT 0;
  SET SQL_SAFE_UPDATES=0;
  SET SESSION tx_isolation='READ-COMMITTED';
  SET @start = 'Start', @end = 'End', @success = ' succeeded,', @failed = ' failed, returned SQL_STATE = ', @error_msg = ', error message = ', @total_rows = ' total row count = '; 
  SET @process_name = 'SP_campaign_history_update', @status_flag_success = 1, @status_flag_failure = 0;
  SET @valuation_date = curdate(); -- may use business date in the future
  SET @MonthNumber = Month(curdate());
  SET @DayNumber = Day(curdate());
  -- Set @intervaldays= 30; 
  SELECT count(*) INTO IsHoliday
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

	IF NotRunFlag = 0 THEN
		-- log the start info
		CALL reporting.SP_process_log(@valuation_date, @process_name, @start, null, 'job is running', null);

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
			SET @process_label ='populate the EPIC phone number into campaign_history', @process_type = 'update';
			
			update reporting.campaign_history ch
			inner join ais.vw_client vc on ch.lms_customer_id=vc.Id
            set ch.home_phone = vc.HomePhone, ch.cell_phone = vc.CellPhone 
			where ch.lms_code = 'EPIC' and ch.business_date >= @valuation_date;
			
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
			SET @process_label ='populate the TDC phone number into campaign_history', @process_type = 'update';
			
			update reporting.campaign_history ch
			inner join LOC_001.ca_Customer tc on ch.lms_customer_id= tc.Cust_ID
            set ch.home_phone = tc.Cust_HPhone, ch.cell_phone = tc.Cust_Mphone
			where ch.lms_code = 'TDC' and ch.business_date >= @valuation_date;
 
            
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
			SET @process_label ='populate the JAG phone number into campaign_history', @process_type = 'update';
			
			update reporting.campaign_history ch
			inner join jaglms.lms_customer_info_flat c on ch.lms_customer_id= c.customer_id
            set ch.home_phone = c.homephone, ch.cell_phone = c.cellphone
			where ch.lms_code = 'JAG' and ch.business_date >= @valuation_date;
 
            
			IF sql_code = '00000' THEN
				GET DIAGNOSTICS rowCount = ROW_COUNT;
				SET return_message = CONCAT(@process_type, @success, @total_rows,rowCount);
				CALL reporting.SP_process_log(@valuation_date, @process_name, @process_label, @process_type, return_message, @status_flag_success);
			ELSE
				SET return_message = CONCAT(@process_type, @failed, sql_code, @error_msg ,sql_msg);
				CALL reporting.SP_process_log(@valuation_date, @process_name, @process_label, @process_type, return_message, @status_flag_failure);
			END IF;
		
		END;
    
-- DAT-1348
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
			SET @process_label ='update the JAG phone number in loc_gc_campaign_history', @process_type = 'update';
			
			update reporting.loc_gc_campaign_history ch
			inner join jaglms.lms_customer_info_flat c on ch.lms_customer_id= c.customer_id
            set ch.home_phone = c.homephone, 
            ch.cell_phone = c.cellphone
			where ch.job_ID like '%JAGLOC%' and ch.list_generation_date >= @valuation_date;
 
            
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
			SET @process_label ='update the JAG phone number in monthly_campaign_history', @process_type = 'update';
			
			update reporting.monthly_campaign_history ch
			inner join jaglms.lms_customer_info_flat c on ch.lms_customer_id= c.customer_id
            set ch.home_phone = c.homephone, 
            ch.cell_phone = c.cellphone
			where ch.lms_code = 'JAG'  and date(ch.list_generation_time) >= @valuation_date;
 
            
			IF sql_code = '00000' THEN
				GET DIAGNOSTICS rowCount = ROW_COUNT;
				SET return_message = CONCAT(@process_type, @success, @total_rows,rowCount);
				CALL reporting.SP_process_log(@valuation_date, @process_name, @process_label, @process_type, return_message, @status_flag_success);
			ELSE
				SET return_message = CONCAT(@process_type, @failed, sql_code, @error_msg ,sql_msg);
				CALL reporting.SP_process_log(@valuation_date, @process_name, @process_label, @process_type, return_message, @status_flag_failure);
			END IF;
		
		END;
   -- DAT-1348 populate the Is_Transactional_optin and Is_SMS_Marketing_optin flag
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
			SET @process_label ='populate the SMS flag into campaign_history', @process_type = 'Update';   

      update reporting.campaign_history ch
				inner join (select list.customer_id, 
										count(if(list.notification_name = 'SMS_TRANSACTIONAL' and list.state = 1, list.customer_id, null)) as 'Transactional With Consent',
										count(if(list.notification_name = 'SMS_TRANSACTIONAL' and list.Txt_Stop = 1, list.customer_id, null)) as 'Transactional Text Stop',   
										count(if(list.notification_name = 'SMS_MARKETING' and list.state = 1, list.customer_id, null)) as 'SMS Marketing With Consent', 
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
						group by list.customer_id) aa on ch.lms_customer_id = aa.customer_id
					set ch.is_transactional_optin = if(aa.`Transactional With Consent`=1 and aa.`Transactional Text Stop`=0,1,0),
								ch.is_sms_marketing_optin = if(aa.`SMS Marketing With Consent`=1 and  aa.`SMS Marketing Text Stop`=0,1,0)
					where ch.lms_code = 'JAG' and ch.business_date >= @valuation_date;
 
   
      IF sql_code = '00000' THEN
				GET DIAGNOSTICS rowCount = ROW_COUNT;
				SET return_message = CONCAT(@process_type, @success, @total_rows,rowCount);
				CALL reporting.SP_process_log(@valuation_date, @process_name, @process_label, @process_type, return_message, @status_flag_success);
			ELSE
				SET return_message = CONCAT(@process_type, @failed, sql_code, @error_msg ,sql_msg);
				CALL reporting.SP_process_log(@valuation_date, @process_name, @process_label, @process_type, return_message, @status_flag_failure);
			END IF;
	##########populate the is_email_transactional_optin and is_email_marketing_optin flag	
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
			SET @process_label ='populate the is_email_transactional_optin and is_email_marketing_optin flag', @process_type = 'Update';   

				Update reporting.campaign_history ch 
					inner join jaglms.lms_customer_info_flat lci on lci.customer_id=ch.lms_customer_id
					set ch.is_email_transactional_optin=if(lci.optout_account_email='false' or lci.optout_account_email is NULL,1,0),
							ch.is_email_marketing_optin=if(lci.optout_marketing_email='false' or lci.optout_marketing_email is NULL,1,0)
					where ch.business_date>= @valuation_date  and ch.lms_code='JAG';


					Update reporting.loc_gc_campaign_history ch 
					inner join jaglms.lms_customer_info_flat lci on lci.customer_id=ch.lms_customer_id 
					set ch.is_email_transactional_optin=if(lci.optout_account_email='false',1,0),
							ch.is_email_marketing_optin=if(lci.optout_marketing_email='false',1,0)
					where ch.list_generation_date>= @valuation_date and ch.job_ID like '%JAGLOC%';

					Update reporting.monthly_campaign_history ch 
					inner join jaglms.lms_customer_info_flat lci on lci.customer_id=ch.lms_customer_id
					set ch.is_email_transactional_optin=if(lci.optout_account_email='false' or lci.optout_account_email is NULL,1,0),
							ch.is_email_marketing_optin=if(lci.optout_marketing_email='false' or lci.optout_marketing_email is NULL,1,0)
					where date(ch.list_generation_time) >= @valuation_date  and ch.lms_code='JAG';



					Update reporting.campaign_history ch 
					inner join ais.vw_client cl on cl.id=ch.lms_customer_id
					set ch.is_email_transactional_optin=if(cl.Email_OperationalOptIn=1,1,0),
							ch.is_email_marketing_optin=if(cl.Email_MarketingOptIn=1,1,0)
					where ch.business_date>= @valuation_date  and ch.lms_code='EPIC';


					Update reporting.monthly_campaign_history ch 
					inner join ais.vw_client cl on cl.id=ch.lms_customer_id
					set ch.is_email_transactional_optin=if(cl.Email_OperationalOptIn=1,1,0),
							ch.is_email_marketing_optin=if(cl.Email_MarketingOptIn=1,1,0)
					where date(ch.list_generation_time) >= @valuation_date  and ch.lms_code='EPIC';


					Update reporting.loc_gc_campaign_history ch 
					inner join LOC_001.ca_Customer_Flags cf on cf.Cust_ID=ch.lms_customer_id 
					set ch.is_email_transactional_optin=if(cf.Flag_ID =2 and cf.Flag_Value=1,0,1),
							ch.is_email_marketing_optin=if(cf.Flag_ID in(2,6) and cf.Flag_Value=1,0,1)
					where ch.list_generation_date>= @valuation_date and ch.job_ID like 'LOC%';

					Update reporting.campaign_history ch 
					inner join LOC_001.ca_Customer_Flags cf on cf.Cust_ID=ch.lms_customer_id 
					set ch.is_email_transactional_optin=if(cf.Flag_ID =2 and cf.Flag_Value=1,0,1),
							ch.is_email_marketing_optin=if(cf.Flag_ID in(2,6) and cf.Flag_Value=1,0,1)
					where ch.business_date>= @valuation_date and ch.lms_code='TDC';
   
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
