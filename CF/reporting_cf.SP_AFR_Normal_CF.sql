DROP PROCEDURE IF EXISTS reporting_cf.SP_AFR_Normal_CF;
CREATE PROCEDURE reporting_cf.`SP_AFR_Normal_CF`(intervaldays int)
BEGIN		
/*************************************************************************************************************************
---    NAME : SP_AFR_Normal_CF
---    DESCRIPTION: script for AFR CF data population
---    this initial version was created by Joyce Li  migrated with exception handling
---    DD/MM/YYYY    By              Comment
---    02/05/2019    Joyce/Eric      initial version  
---    03/07/2019                    DAT-908 update the logic for is_external_nc and channel
---    11/07/2019                    DAT-927 add additional columns for AFR
---    12/122019									   DAT-1271 update SUB_ID in 'reporting_cf.SP_AFR_Normal_CF'
****************************************************************************************************************************/
 	-- set session binlog_format='statement' ;
  SET SQL_SAFE_UPDATES=0;
  SET @start = 'Start', @end = 'End', @success = ' succeeded,', @failed = ' failed, returned SQL_STATE = ', @error_msg = ', error message = ', @total_rows = ' total row count = '; 
  SET @process_name = 'SP_AFR_Normal_CF', @status_flag_success = 1, @status_flag_failure = 0;
  SET @valuation_date = curdate(); -- may use business date in the future
  SET SQL_big_selects=1;
 
	SELECT HOUR(CURTIME()) INTO @runhour;
  IF @runhour < 6 or @runhour > 18 THEN
    Set @intervaldays=60; -- capture any missing data in last 60 days before 6am run
	  ELSE
     Set @intervaldays=10; -- if the run time is in office hour, then only capture last 5 days data
  END IF; 
 
   SET @std_date= subdate(curdate(), interval @intervaldays day),
			@end_date= curdate()+1,
			@debit_cutoff_date=
				if(weekday(curdate()) in (0,1),Date_Sub(curdate(), interval 11 day), 
				if(weekday(curdate()) in (2,3,4,5), Date_Sub(curdate(), interval 9 day),
				Date_Sub(curdate(), interval 10 day)));  

  -- log the start info
  CALL reporting.SP_process_log(@valuation_date, @process_name, concat(@start, ' for ', @std_date), null, 'job is running', null);
  	TRUNCATE TABLE reporting_cf.`stg_AFR_payment`;
    TRUNCATE TABLE reporting_cf.`stg_AFR_Normal`;
    TRUNCATE TABLE reporting_cf.`stg_AFR_aba_bank_account`;
  BEGIN
    -- Declare variables to hold diagnostics area information
  	DECLARE sql_code CHAR(5) DEFAULT '00000';
  	DECLARE sql_msg TEXT;
  	DECLARE rowCount INT;
  	DECLARE return_message TEXT;
  	DECLARE CONTINUE HANDLER FOR SQLEXCEPTION
  	BEGIN
  		GET DIAGNOSTICS CONDITION 1
  			sql_code = RETURNED_SQLSTATE, sql_msg = MESSAGE_TEXT;
  	END;
    SET @process_label ='prepare JAG payment data in staging table', @process_type = 'Insert'; 
		insert into reporting_cf.`stg_AFR_payment` (`lms_code`, `loanid`, `DebitDate`, `IsDefaulted`, Debit_Amount, uncollected_amount, defaulted_principal,  `PrincipalPaid` ,  `FeesPaid`)
			select 'JAG' as lms_code,  
				inn.loanid,
				psi2.item_date as DebitDate,
				if(psi2.status in ('Return', 'Missed'),1,0) as IsDefaulted
        , psi2.total_amount as Debit_Amount #Joyce
        , if(psi2.status in ('Return', 'Missed'), psi2.total_amount,0) as uncollected_amount #Joyce
        , if(psi2.status in ('Return', 'Missed'), psi2.amount_prin,0) as defaulted_principal-- added on Mar08,2018 #Joyce
        
				,if(psi2.status in('Cleared','Correction'),psi2.amount_prin,0) as PrincipalPaid,
				if(psi2.status in('Cleared','Correction'), psi2.total_amount-psi2.amount_prin,0) as FeesPaid 
			from reporting_cf.vcf_lms_payment_schedule_items psi2 
			join (
					select 
						la.origination_loan_id as loanid
						, min(psi.payment_schedule_item_id) as psi_id
						FROM reporting_cf.leads_accepted la
						join reporting_cf.vcf_lms_payment_schedules ps on la.origination_loan_id=ps.base_loan_id
						join reporting_cf.vcf_lms_payment_schedule_items psi on ps.payment_schedule_id = psi.payment_schedule_id
						where 
						la.lms_code='JAG'
						and la.received_time between @std_date and @end_date
						and (la.isuniqueaccept = 1 or (la.loan_sequence=1 and la.isexpress=0))
						and psi.status in ('Cleared', 'Missed', 'Return', 'Correction')
						-- and (psi.item_type is null or psi.item_type='D')
                        and psi.total_amount > 0 -- DAT-765
						and ps.is_collections=0
						group by la.origination_loan_id) inn
						on psi2.payment_schedule_item_id=inn.psi_id;
		-- log the process
		IF sql_code = '00000' THEN
			GET DIAGNOSTICS rowCount = ROW_COUNT;
			SET return_message = CONCAT(@process_type, @success, @total_rows,rowCount);
			CALL reporting.SP_process_log(@valuation_date, @process_name, @process_label, @process_type, return_message, @status_flag_success);
		ELSE
			SET return_message = CONCAT(@process_type, @failed, sql_code, @error_msg ,sql_msg);
			CALL reporting.SP_process_log(@valuation_date, @process_name, @process_label, @process_type, return_message, @status_flag_failure);
		END IF;
  
  END;
  -- DAT-443
  BEGIN
    -- Declare variables to hold diagnostics area information
  	DECLARE sql_code CHAR(5) DEFAULT '00000';
  	DECLARE sql_msg TEXT;
  	DECLARE rowCount INT;
  	DECLARE return_message TEXT;
  	DECLARE CONTINUE HANDLER FOR SQLEXCEPTION
  	BEGIN
  		GET DIAGNOSTICS CONDITION 1
  			sql_code = RETURNED_SQLSTATE, sql_msg = MESSAGE_TEXT;
  	END;
    SET @process_label ='prepare aba and bank account data in staging table', @process_type = 'Insert';           
		insert into reporting_cf.`stg_AFR_aba_bank_account` (`lms_code`,`lead_sequence_id`, `lms_application_id`, `Is_ABA_Changed` , `Is_Account_Number_Changed`, `insert_datetime`)
    select la.lms_code,	la.lead_sequence_id, la.lms_application_id,
			case when ifnull(la.aba, '') = ifnull(a.routingnumber, '') then 0
			-- when ifnull(la.aba, '') <> ifnull(a.routingnumber, '') then 1
			else 1 
			end as Is_ABA_Changed,       
			case when  ifnull((Select f.account_number from reporting_cf.vcf_lms_customer_info_flat f where f.customer_id=la.lms_customer_id and la.lms_code = 'JAG' limit 1),'')  =ifnull(SUBSTRING_INDEX(CONVERT(AES_DECRYPT(a.bankaccount,'09qewkjlnasdfiuasdjnq2r09iqweklmnagu0q92310x109cm901c212cn9129rn9hr'),CHAR(100)),':',-1),'') then 0
				-- when ifnull((Select f.account_number from jaglms.lms_customer_info_flat f where f.customer_id=la.lms_customer_id and la.lms_code = 'JAG' limit 1),'')  <> ifnull(SUBSTRING_INDEX(CONVERT(AES_DECRYPT(a.bankaccount,'09qewkjlnasdfiuasdjnq2r09iqweklmnagu0q92310x109cm901c212cn9129rn9hr'),CHAR(100)),':',-1),'') then 1
				else 1 
			end as Is_Account_Number_Changed, 
      now()
			FROM reporting_cf.leads_accepted la 
			left outer join datawork.mk_application a on a.lead_sequence_id=la.lead_sequence_id
			where	la.lms_code='JAG'
				and la.application_status !='Pending (wth New Loan After)'
				-- and la.received_time between @std_date and @end_date
        and la.received_time between @std_date and @end_date 
				and (la.isuniqueaccept = 1 or (la.loan_sequence=1 and la.isexpress=0))
				and la.IsApplicationTest=0
        and la.lead_sequence_id > 0
        -- due to there are some duplicate lead_sequence_id, so need to filter it out 
        and la.lead_sequence_id not in (select aa.lead_sequence_id from (select lead_sequence_id, count(*) from reporting_cf.leads_accepted 
                where lms_code='JAG' and lead_sequence_id > 0  
										  and received_time between @std_date and @end_date 
								group by lead_sequence_id having count(*) > 1)aa )
			union
      select la.lms_code,	la.lead_sequence_id, la.lms_application_id,
				case when ifnull(la.aba, '') = ifnull(a.routingnumber, '') then 0
						--  WHEN ifNULL(la.aba, '') <> ifnull(a.routingnumber, '') then 1
						else 1
				end as Is_ABA_Changed,
				case when ifnull((select cast(aes_decrypt(cb.Cust_Acct_No,SHA('109480123k0asdojf2309434joweg0oijdf0oitqqq')) as char) from LOC_001.ca_Customer_Bank cb where la.lms_customer_id = cb.cust_id and la.lms_code = 'TDC' limit 1),'')= ifnull(SUBSTRING_INDEX(CONVERT(AES_DECRYPT(a.bankaccount,'09qewkjlnasdfiuasdjnq2r09iqweklmnagu0q92310x109cm901c212cn9129rn9hr'),CHAR(100)),':',-1),'' )  then 0
						 when ifnull((select cast(aes_decrypt(cl.CustomerAccount,SHA('109480123k0asdojf2309434joweg0oijdf0oitqqq')) as char) from ais.vw_client cl where cl.id = la.lms_customer_id and la.lms_code = 'EPIC' limit 1),'')= ifnull(SUBSTRING_INDEX(CONVERT(AES_DECRYPT(a.bankaccount,'09qewkjlnasdfiuasdjnq2r09iqweklmnagu0q92310x109cm901c212cn9129rn9hr'),CHAR(100)),':',-1),'' )  then 0
						--  WHEN ifnull((select cast(aes_decrypt(cb.Cust_Acct_No,SHA('109480123k0asdojf2309434joweg0oijdf0oitqqq')) as char) from LOC_001.ca_Customer_Bank cb where la.lms_customer_id = cb.cust_id and la.lms_code = 'TDC' limit 1),'') <> ifnull(SUBSTRING_INDEX(CONVERT(AES_DECRYPT(a.bankaccount,'09qewkjlnasdfiuasdjnq2r09iqweklmnagu0q92310x109cm901c212cn9129rn9hr'),CHAR(100)),':',-1),'' )   then 1
						--  when ifnull((select cast(aes_decrypt(cl.CustomerAccount,SHA('109480123k0asdojf2309434joweg0oijdf0oitqqq')) as char) from ais.vw_client cl where cl.id = la.lms_customer_id and la.lms_code = 'EPIC' limit 1),'')<> ifnull(SUBSTRING_INDEX(CONVERT(AES_DECRYPT(a.bankaccount,'09qewkjlnasdfiuasdjnq2r09iqweklmnagu0q92310x109cm901c212cn9129rn9hr'),CHAR(100)),':',-1),'' )  then 1
				else 1
				end as Is_Account_Number_Changed,
        now()
				FROM reporting_cf.leads_accepted la
				left outer join datawork.mk_application a on a.lead_sequence_id=la.lead_sequence_id
			where la.lms_code in ('EPIC', 'TDC')
				-- and la.received_time between @std_date and @end_date
        and la.received_time between @std_date and @end_date
				and (la.isuniqueaccept = 1 or (la.loan_sequence=1 and la.isexpress=0))
				and la.IsApplicationTest=0 
        and la.lead_sequence_id > 0
        -- due to there are some duplicate lead_sequence_id, so need to filter it out 
        and la.lead_sequence_id not in (select aa.lead_sequence_id from (select lead_sequence_id, count(*) from reporting_cf.leads_accepted 
                where lms_code in ('EPIC', 'TDC') and lead_sequence_id > 0  
                  and received_time between @std_date and @end_date 
								group by lead_sequence_id having count(*) > 1)aa );
 
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
    SET @process_label ='create temporary tables', @process_type = 'Create';
    
		DROP TEMPORARY TABLE IF EXISTS temp_users;
		CREATE TEMPORARY TABLE IF NOT EXISTS temp_users ( INDEX(lms_application_id) ) 
		AS (
			select la.received_time, la.lms_customer_id, la.lms_application_id, cast(la.loan_number as unsigned) as loan_id, max(u.id) as user_id, if(sum(u.isExternal)>0, 1,0) IsExternal
             , if(sum(u.verified)>0,1,null)  as Is_Verified-- added Mar08,2018 #Joyce
			FROM reporting_cf.leads_accepted la 
			join webapi.users u on la.lms_customer_id =u.lms_customer_id and la.campaign_name is not null and la.campaign_name !='MK-WEB-RC'

			where
			la.lms_code='JAG'
			and la.application_status not in ('Pending (wth New Loan After)')
      and la.received_time between @std_date and @end_date
			and (la.isuniqueaccept = 1 or (la.loan_sequence=1 and la.isexpress=0))
			group by la.lms_customer_id, la.lms_application_id -- edited Mar30,2018 #Joyce
			);

		/* view behavior */
		DROP TEMPORARY TABLE IF EXISTS temp_views;
		CREATE TEMPORARY TABLE IF NOT EXISTS temp_views ( INDEX(lms_application_id) ) 
		AS (
				select la.lms_customer_id, la.lms_application_id, max(if(tr.page in ('welcome', 'contact','income'), 1,0)) as wel_visit, max(if(tr.page='cbs-form', 1,0)) as cbs_visit,
				max(if(tr.page in ('exteral_create_password', 'external_create_password'),1,0)) as create_pw_page_visit, 
				max(if(tr.page in ('emailConfirmation', 'email_Confirmation'),1,0)) as email_confirm_page_visit
				from reporting_cf.leads_accepted la
				join webapi.tracking tr on la.lead_sequence_id = tr.lead_sequence_id 
				where
				la.lms_code='JAG'
				and la.application_status not in ('Pending (wth New Loan After)')
				and la.received_time between @std_date and @end_date
				and (la.isuniqueaccept = 1 or (la.loan_sequence=1 and la.isexpress=0))
				group by  la.lms_customer_id, la.lms_application_id                                                                                                                             
				);


		/* submit behavior */

		DROP TEMPORARY TABLE IF EXISTS temp_sub;
		CREATE TEMPORARY TABLE IF NOT EXISTS temp_sub ( INDEX(lms_application_id) ) 
		AS (
				select 
				u.received_time, u.lms_application_id,
				sum(IF(li.loan_key='external_amount',1,0)) ex_page1,
				sum(IF(li.loan_key='contact',1,0)) org_page1,
				sum(IF(li.loan_key='income',1,0)) org_page2,
				sum(IF(li.loan_key='loan-cbs',1,0)) cbs_page

				FROM temp_users u
				join webapi.loan_info li on u.user_id=li.user_id and u.loan_id=li.loan_id
				group by u.lms_application_id
				);


		/* agreement behavior */
		DROP TEMPORARY TABLE IF EXISTS temp_agree;
		CREATE TEMPORARY TABLE IF NOT EXISTS temp_agree ( INDEX(loan_id) ) 
		AS (
				/*select u.lms_application_id,
				max(ag.doc_order)+1 as Ag_Total_Page, 
				sum(ag.completed) as Ag_Completed_Page,
				sum(if(ag.completed=0,1,0)) as Is_incompleted
				from  webapi.loan_agreements ag
				join temp_users u on ag.loan_id=u.loan_id and ag.user_id=u.user_id
				join temp_sub s on u.lms_application_id=s.lms_application_id and if(s.received_time<='2017-04-28', s.cbs_page>0, s.cbs_page in(0,1)) */
				select ag.loan_id,
				max(ag.doc_order)+1 as Ag_Total_Page, 
				sum(ag.completed) as Ag_Completed_Page,
				sum(if(ag.completed=0,1,0)) as Is_incompleted
				from  webapi.loan_agreements ag	
           inner join webapi.users u on ag.user_id = u.id and u.organization_id = 2
				group by ag.loan_id
				);

		-- log the process
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
    SET @process_label ='populate AFR in staging table', @process_type = 'Create';

   ##question: for phone ops information, do we update all fields or create a temporary table for clean phone number???
		INSERT INTO reporting_cf.stg_AFR_Normal(
			`lead_id`,
			`nc_lms_accepts`,
			`rc_lms_accepts`,
			`is_returning`,
			`is_unique_accepts`,
			`is_unique_requests`,
			`is_express`,
			`is_overnight_lead`, ##
			`is_external_nc`,
			`rc_external`,
			`is_epic_rc`,
			`channel`,
			`is_transactional_optin`,
			`is_sms_marketing_optin`,
			`lms_code`,
			`product`,
			`state`,
			`customer_id`,
			`original_lead_id`,
			`loan_application_id`,
			`lms_display_number`,
			`origination_loan_id`,
			`lead_sequence_id`,
			`loan_sequence`,
			`gclid`,
			`Days_Ago`, 
			`WD_Reason`,
			`Is_RAL`,
			`n_prev_accepts`, 
			`organic_group`,
			`organic_sub_group`,
			`provider_name`,
			`campaign_name`,
			`DM_Name`, 
			`is_#campaign`,
			`lead_cost`,
			`store_name`,
			`loan_lender`,
			`lender_group`,
			`application_status`,
			`current_loan_status`,
			`loan_app`,
			`lead_received_time`,
			`original_lead_received_date`,
			`Weekday_Name`,
			`original_lead_received_month`,
			`original_lead_received_day`,
			`original_lead_received_hour`,
			`final_application_date`,
			`final_application_month`,
			`final_application_day`,
			`final_application_hour`,
			`app_results`,
			`is_originated`,
			`origination_datetime`,
			`origination_date`,
			`effective_date`,
			`minute_to_origination`,
			`day_to_origination`,
			`same_day_origination`,
			`1_day_origination`,
			`2_day_origination`,
			`3_day_origination` ,
			`3+_day_origination`,
			`withdrawn_reason`,
			`withdrawn_reason_detail`,
			`withdrawn_datetime` ,
			`minute_to_withdrawn`,
			`same_day_withdrawn`,
			`1_day_withdrawn`,
			`2_day_withdrawn`,
			`3_day_withdrawn`,
			`3+_day_withdrawn`,
			`Loan_performance`,
			`loan_term`,
			`1st_payment_due_date` ,
			`is_1st_payment_debited`,
			`1st_payment_debit_date`,
			`is_1st_payment_defaulted`,
			`principal_paid`,
			`fees_paid`,
			`Is_PIF` , 
			`Total_Principal_for_Debited`, 
			`Total_Principal_for_1stDefaulted` , 
			`ach_auth`,
			`extra_app_info`,
			`Is_Download_App`,
			`pay_frequency`,
			`pay_frequency_hb`,
			`lms_requested_amount`,
			`hotbox_requested_amount` ,
			`hotbox_requested_amount_orig`,
			`net_monthly_income`,
      `hotbox_nmi`, -- DAT-927
			`hotbox_net_monthly_income`,
			`Net_Monthly_Income_Update`, 
			`Current_Net_Monthly_Income`, 
			`application_approved_amount`,
			`originated_loan_amount`,
			`loan_limit_no_cap`,
			`limit_hardcap`,
			`loan_limit`,
			`loan_amt_in_welcomepage_cap` ,
			`loan_amt_in_welcomepage_no_cap`,
			`days_from_last_application`,
			`days_from_last_paid_Off`,
			`ref_url`,
			`is_blue_light_special`,
			`is_ft_uw_stream`,
			`uw_stream_id`,
			`uw_stream_name`,     
			`aff_id`,
			`sub_id`,
			`hotbox_pay_type`, 
			`JAG_paytype`,
			`paycheck_amount`,
			`ebureau_conv`,
			`ebureau_gen`,
			`ebureau_gen2`,
			`ebureau_rev`,
			`CF_Score`,  
			`CBB_Score2`,
			`mk_risk_score`,
			`mk_conv_score`,
			`mk_nocontact_score`,
			`FT20_score`,
			`mk_ft_tu_base_model_score`,
			`RULE_DESCRIPTION`,
			`hb_processing_time`,
			`test`,
			`employment_and_pay`, #rename to `Employment_and_Pay`
			`manual_bank_ver_status`,     
			`actual_no_bank_call`,
			`aba` ,
			`hotbox_aba`, 
			`Is_ABA_Changed`,
			`account_type`,
			`Is_Account_Number_Changed`,
			`hotbox_last_pay_date`,
			`hotbox_next_pay_date`,
			`hotbox_second_pay_date`,
			`hotbox_payday_term1`,
			`hotbox_payday_term2`,
			`customer_age`,
			`email_address`,
			`email_domain`,
			`rental_status`,
			`city`,
			`zip`,
			`hbzip3`,
			`hbzip4`,
			`employment_type`,
			`income_source`,
			`bank_name`,
			`emp_name`,
			`empzip`,   
      `job_title`,
      `bank_months`, -- DAT-927
			`bank_years`, 
			`address_months`,
			`address_years`,
			`emp_months`,
			`emp_years`,
			`Phone_Ops`,
			`homephone`,
			`cellphone`,
			`hphone_ephone_match_flag`,
			`update_datetime`) 
		SELECT Distinct
		 'Lead_ID' as lead_id,
      if(la.loan_sequence=1 and la.isexpress=0, 1,0) as nc_lms_accepts,
			if(la.loan_sequence>1  and la.isuniquerequest=1, 1,0) as rc_lms_accepts,
      if(la.loan_sequence>1, 1,0) as is_returning,
			ifnull(la.isuniqueaccept,0)  as is_unique_accepts,
			ifnull(la.isuniquerequest,0)  as is_unique_requests,
			ifnull(if(la.origination_loan_id is null, la.isexpress, la2.isexpress),0) as Is_Express,
			ifnull(if(la.origination_loan_id is null,la.IsOverNight,la2.IsOverNight),0) as Is_Overnight_Lead,
     --  if(la.provider_name not in('Money Key Web', 'Money Key External') and la.loan_sequence=1 and la.isexpress=0,1,0) as is_external_nc,
      if(la.provider_name !='CreditFresh Internal' and la.loan_sequence=1 and la.isexpress=0,1,0) as is_external_nc, -- DAT-908
      if(mam.RC_FROM_LEAD='true',1, null) as rc_external,
      -- if(sum(users.is_epic_rc)>0,1,0) as is_epic_rc, -- the sum function caused the group to one record
      if(users.is_epic_rc>0,1,0) as is_epic_rc, 
      -- if(la.provider_name not in('Money Key Web', 'Money Key External'), 'External', 'Internal') as Channel,
      if(la.provider_name!='CreditFresh Internal', 'External', 'Internal') as Channel, -- DAT-908
      if(marketing.`Transactional With Consent`=1 and  marketing.`Transactional Text Stop`=0,1,0) as Is_Transactional_optin,
      if(marketing.`SMS Marketing With Consent`=1 and  marketing.`SMS Marketing Text Stop`=0,1,0) as Is_SMS_Marketing_optin,
			la.lms_code,
			la.product,
			la.state,
      la.lms_customer_id,
			la.lms_application_id as original_lead_id,
			if(la.Origination_Loan_ID is null, la.lms_application_id, la2.lms_application_id) as Loan_Application_ID,
			if(la.origination_loan_id is null, la.loan_number, la2.loan_number) as LMS_Display_Number,
			la.Origination_Loan_ID as Origination_Loan_ID,
      la.lead_sequence_id, 
      la.loan_sequence,
      if(mam.GCLID is not null, mam.GCLID, wc.click_value) as gclid,
      la.Days_Ago,
			la.WD_Reason,
			la.Is_RAL,
      la.n_prev_accepts, 
      ls.organic_group,
      ls.organic_sub_group,
			la.provider_name,
			la.campaign_name,
			(select dm.dm_name from reporting.dm_campaign_mapping dm 
				 where date(la.received_time) between date_sub(dm.start_date, interval 5 day) and dm.expire_date
					 and la.affid like trim(dm.affid) limit 1
      ) as DM_Name,  ##Joyce
			if(la.campaign_name is null, null, if(la.Campaign_Name like '%#%',1,0)) as 'Is_#Campaign',
      la.lead_cost as Lead_Cost,
			la.storename,
      if(la.origination_loan_id is null, la.portfolio_name, la2.portfolio_name) as Loan_Lender,
      if(la.state in('OH', 'TX') and la.storename like '%BAS%', 'BASTION', if(la.state in('OH', 'TX') and la.storename like '%NCP%', 'NCP', '')) as Lender_group,
      if(la.origination_loan_id is null, la.application_status, la2.application_status) as Application_Status,
      la2.loan_status as 'Current_Loan_Status',
      'Loan_APP', 
      la.received_time,
			date(la.received_time) as Original_Lead_Received_Date,
      dayname(la.received_time) as Weekday_Name, 
			date_format(la.received_time, '%Y %M') as Original_Lead_Received_Month,
			day(la.received_time) as Original_Lead_Received_Day,
			hour(la.received_time) as Original_Lead_Received_Hour,
			if(la.Origination_Loan_ID is null, date(la.received_time), date(la2.received_time)) as Final_Application_Date,
			if(la.Origination_Loan_ID is null, date_format(la.received_time, '%Y %M'), date_format(la2.received_time, '%Y %M')) as Final_Application_Month,
			if(la.Origination_Loan_ID is null, day(la.received_time), day(la2.received_time)) as Final_Application_Day,
			if(la.Origination_Loan_ID is null, hour(la.received_time), hour(la2.received_time)) as Final_Application_hour, 
      'APP_Results',
			if(la.origination_loan_id is null, 0, 1) as Is_Originated,
			la2.origination_time as origination_datetime,
      date(la2.origination_time) as origination_date,
			la2.effective_date ,
			if(la.origination_loan_id is null,'', timestampdiff(minute,la.received_time, la2.origination_time)) as minute_to_origination,
			if(la.origination_loan_id is null,'', timestampdiff(day, date(la.received_time), date(la2.origination_time))) as day_to_origination,
			if(la.origination_loan_id is null,0, 
			if(timestampdiff(day, date(la.received_time), date(la2.origination_time))=0, 1,0)) as Same_Day_Origination, 
			if(la.origination_loan_id is null,0, 
			if(timestampdiff(day, date(la.received_time), date(la2.origination_time))=1, 1,0)) as 1_Day_Origination,
			if(la.origination_loan_id is null,0, 
			if(timestampdiff(day, date(la.received_time), date(la2.origination_time))=2, 1,0)) as 2_Day_Origination,
			if(la.origination_loan_id is null,0, 
			if(timestampdiff(day, date(la.received_time), date(la2.origination_time))=3, 1,0)) as 3_Day_Origination,
			if(la.origination_loan_id is null,0, 
			if(timestampdiff(day, date(la.received_time), date(la2.origination_time))>3, 1,0)) as '3+_Day_Origination',
			if(la.origination_loan_id>0, null, wrr.withdrawn_reason) as withdrawn_reason,
			if(la.origination_loan_id>0, null, la.withdrawn_reason) as withdrawn_reason_detail,
			if(la.origination_loan_id>0, null, la.withdrawn_time) as withdrawn_datetime,
			if(la.withdrawn_time is null, null, timestampdiff(minute, la.received_time,la.withdrawn_time)) as minute_to_withdrawn,   
      if(la.origination_loan_id>0, 0, if(timestampdiff(day, date(la.received_time), date(la.withdrawn_time))=0, 1,0)) as Same_Day_Withdrawn,
      if(la.origination_loan_id>0, 0, if(timestampdiff(day, date(la.received_time), date(la.withdrawn_time))=1, 1,0)) as 1_Day_Withdrawn,
      if(la.origination_loan_id>0, 0, if(timestampdiff(day, date(la.received_time), date(la.withdrawn_time))=2, 1,0)) as 2_Day_Withdrawn,
      if(la.origination_loan_id>0, 0, if(timestampdiff(day, date(la.received_time), date(la.withdrawn_time))=3, 1,0)) as 3_Day_Withdrawn,
      if(la.origination_loan_id>0, 0, if(timestampdiff(day, date(la.received_time), date(la.withdrawn_time))>3, 1,0)) as '3+_Day_Withdrawn',
      'Loan_Performance',
      datediff(if(la.origination_loan_id is null, null, lbl.due_date), la2.effective_date) as Loan_Term, 
  		if(la.origination_loan_id is null, null, date(lbl.due_date)) as 1st_payment_due_date,
  		if(la.origination_loan_id is null, 0,if(jp.loanid is not null,1,0)) as is_1st_payment_debited,
  		if(la.origination_loan_id is null, null, date(jp.DebitDate)) as 1st_payment_debit_date,
  		ifnull(if(la.origination_loan_id is null, null, jp.IsDefaulted),0) as is_1st_payment_defaulted,
  		if(la.origination_loan_id is null, null, jp.PrincipalPaid) as principal_paid,
  		if(la.origination_loan_id is null, null, jp.FeesPaid) as fees_paid,
      if(jp.PrincipalPaid=la2.approved_amount, 1, 0) as Is_PIF,
      if(la.origination_loan_id is null, null,if(jp.loanid is not null, la2.approved_amount, null)) as Total_Principal_for_Debited, 
      if(la.origination_loan_id is null, null,if(jp.IsDefaulted=1, la2.approved_amount, null)) as Total_Principal_for_1stDefaulted, 
      la.ACH_AUTH,
      'Extra_App_Info',
      if(app.sso_username is not null,1,0) as Is_Download_App,
      ifnull(la.pay_frequency, '') as Pay_Frequency,
      ifnull(a.payfrequency, '') as Pay_Frequency_HB,
      if(la.Origination_Loan_ID is null,la.requested_amount,la2.requested_amount) as LMS_Requested_Amount,
		  a.requestedamount as hotbox_requested_amount,
		  a.orig_amt_requested as hotbox_requested_amount_orig,
      la.netmonthlyincome  as Net_Monthly_Income,
      replace(replace(a.nmi,'$',''), ',','') as hotbox_nmi, -- DAT-927 
      a.netmonthly as hotbox_net_monthly_income, 
      la.netmonthlyincome_update as Net_Monthly_Income_Update,
      lcif.nmi as Current_Net_Monthly_Income,
      if(la.origination_loan_id is null,la.approved_amount,la2.approved_amount) as Application_Approved_Amount, 
      la2.approved_amount as Originated_Loan_Amount,
      if(la.origination_loan_id is null,la.LoanLimitNoCap,la2.LoanLimitNoCap)  as Loan_Limit_No_Cap,
		  la.HardCap  as Limit_HardCap,
		  if(la.origination_loan_id is null,la.MaxLoanLimit,la2.MaxLoanLimit)  as Loan_Limit,
      null as `loan_amt_in_welcomepage_cap` , 
  	  null as `loan_amt_in_welcomepage_no_cap`,
      datediff(la.received_time,
			(select la3.received_time from reporting_cf.leads_accepted la3 where la3.lms_customer_id=la.lms_customer_id
			and la3.lms_code=la.lms_code 
			and la3.isuniqueaccept=1
			and la3.received_time < la.received_time order by la3.received_time desc limit 1)) as Days_From_Last_Application,
			datediff(la.received_time,
			(select la3.last_paymentdate from reporting_cf.leads_accepted la3 where la3.lms_customer_id=la.lms_customer_id
			and la3.loan_status in ('Paid Off Loan', 'Paid Off', 'Charged Off Paid Off', 'Returned Item Paid Off', 'Settlement Paid Off')
			and la3.lms_code=la.lms_code
			and la3.received_time < la.received_time order by la3.received_time desc limit 1)) as Days_From_Last_Paid_Off, 
			a.refurl as ref_url,
			(case when a.`Business Rule Set Pass`='Blue Light Special' then 1 else 0 end) as Is_Blue_Light_Special,
			(case when a.uw_stream = 25 then 1 else 0 end) as Is_FT_UW_Stream,
			a.UW_STREAM_ID,
			us.description,    
		  if(la.origination_loan_id is null, la.affid, la2.affid)  as Aff_ID,
		/* (case
    			when la.origination_loan_id is null and la.provider_name = 'Lead Flash' then left(la.affid,4)
    			when la.origination_loan_id is null and la.provider_name != 'Lead Flash' then la.subid
    			when la.origination_loan_id is not null and la.provider_name = 'Lead Flash' then left(la2.affid,4)
    			when la.origination_loan_id is not null and la.provider_name != 'Lead Flash' then la2.subid
    			else 'error'
			end)as SUB_ID, */
      (case
				when la.origination_loan_id is null and la.provider_name in ('Lead Flash CF', 'Lead Flash CF SF') then left(la.affid,4)
				when la.origination_loan_id is null and la.provider_name not in ('Lead Flash CF', 'Lead Flash CF SF') then la.subid
				when la.origination_loan_id is not null and la.provider_name in ('Lead Flash CF', 'Lead Flash CF SF') then left(la2.affid,4)
				when la.origination_loan_id is not null and la.provider_name not in ('Lead Flash CF', 'Lead Flash CF SF') then la2.subid
				else 'error'
			end)as SUB_ID, -- DAT-1271
      a.paytype as hotbox_pay_type, 
      lcif.paytype as JAG_paytype,
		 la.paycheck  as Paycheck_Amount,
		 eb.`/ebureau/conv`  as ebureau_conv, 
		 eb.`/ebureau/gen`  as ebureau_gen,
		 eb.`/ebureau/gen2`  as ebureau_gen2,
		 eb.`/ebureau/rev`  as  ebureau_rev,
     cf.`/x/cf/cf-score` AS CF_Score,  
		 cbb.`/x/cbb/cbb-score2` AS CBB_Score2, 
     mam.mk_risk_score,
     mam.mk_conv_score,
		 mam.mk_nocontact_score,
     ft.`/ft/ar/ti/s/ScoreDetail/Score` as 'FT20_score',
     mam.mk_ft_tu_base_model_score,
		 a.RULE_DESCRIPTION,
     round(a.processingtime/1000,2) as hb_processing_time,
     a.test,
    'Employment_and_Pay',
    (case when la.isreturning >= 1 then 'RC'  -- DAT-476
         when  ifnull(if(la.origination_loan_id is null, la.NO_BANK_CALL, la2.NO_BANK_CALL),0) = 0  then 'BV' -- DAT-484 
         when  ifnull(if(la.origination_loan_id is null, la.NO_BANK_CALL, la2.NO_BANK_CALL),0) = 1 and (ababa.Is_ABA_Changed=1 or ababa.Is_Account_Number_Changed=1) then 'BV'
         when  ifnull(if(la.origination_loan_id is null, la.NO_BANK_CALL, la2.NO_BANK_CALL),0) = 1 and ababa.Is_ABA_Changed=0 and ababa.Is_Account_Number_Changed=0 then 'NBV'
         else NULL
     end) as  'MANUAL_BANK_VER_STATUS',    
		ifnull(if(la.origination_loan_id is null, la.NO_BANK_CALL, la2.NO_BANK_CALL),0) as actual_no_bank_call,
    la.aba as aba,
    a.routingnumber as hotbox_aba, 
    ifnull(ababa.`Is_ABA_Changed`, 0) as Is_ABA_Changed,
    a.accounttype as Account_Type,
		ifnull(ababa.`Is_Account_Number_Changed`,0) as Is_Account_Number_Changed,
		a.lastpaydate as hotbox_last_pay_date,
		a.nextpaydate as hotbox_next_pay_date,
		a.secondpaydate as hotbox_second_pay_date,
		datediff(a.nextpaydate,a.lastpaydate) as hotbox_payday_term1,
		datediff(a.secondpaydate, a.nextpaydate) as hotbox_payday_term2,
		la.age as Customer_Age,
		la.emailaddress as email_address,
		SUBSTR(SUBSTR(la.emailaddress, INSTR(la.emailaddress, '@'), INSTR(la.emailaddress, '.')), 2) as Email_Domain,
		a.rentorown as Rental_Status,
		a.city,
		a.zip,
    left(a.zip, 3) as hbzip3,
    left(a.zip, 4) as hbzip4,
		la.employmenttype as Employment_Type,
		a.incometype as Income_Source,
		la.bankname as Bank_Name,
    ifnull(a.empname, '') as emp_name,
    a.empzip,
		ifnull(a.jobtitle, '') as job_title,
    a.BANKMONTHS as bank_months,  -- DAT-927
		a.BANKYEARS as bank_years,
		a.addressmonths as address_months, 
		a.addressyears as address_years, 
		a.empmonths as emp_months, 
		a.empyears  as emp_years,
		null as Phone_Ops,
		null as homephone,
		null as cellphone,
		null as hphone_ephone_match_flag,
    now()
		FROM reporting_cf.leads_accepted la 
     inner join jaglms.lms_customer_info_flat lcif on la.lms_customer_id=lcif.customer_id and lcif.organization_id = 2
		 left join jaglms.lms_base_loans lbl on la.origination_loan_id=lbl.base_loan_id
		left outer join reporting_cf.leads_accepted la2 on la.lms_customer_id=la2.lms_customer_id and la.origination_loan_id=cast(la2.loan_number as unsigned) and la2.application_status != 'Voided' and la2.lms_code='JAG'
		left outer join reporting_cf.stg_AFR_payment jp on jp.lms_code = 'JAG' and la.origination_loan_id=jp.loanid
		left outer join reporting.withdrawn_reason_code wrr on la.withdrawn_reason_code=wrr.withdrawn_reason_code
		left outer join datawork.mk_application a on a.lead_sequence_id=la.lead_sequence_id
		left outer join datawork.mk_application_more mam on mam.lead_sequence_id=la.lead_sequence_id
		left outer join jaglms.uw_master_streams us on a.UW_STREAM_ID=us.master_stream_id
		left outer join datawork.mk_ebureau eb on la.lead_sequence_id = eb.lead_sequence_id
		left join temp_users u on la.lms_application_id=u.lms_application_id
		left join temp_views v on la.lms_application_id=v.lms_application_id
		left join temp_sub s on la.lms_application_id=s.lms_application_id
    -- 	left join temp_agree ag on ag.lms_application_id=la.lms_appication_id 
		left join temp_agree ag on ag.loan_id=la.loan_number -- DAT-537
   --  left join reporting_cf.vcf_lms_customer_info_flat lcif on la.lms_customer_id=lcif.customer_id #Joyce
    left join reporting_cf.stg_AFR_aba_bank_account ababa on la.lead_sequence_id = ababa.lead_sequence_id 
    LEFT OUTER JOIN datawork.mk_clearfraud cf ON la.Lead_Sequence_ID = cf.lead_sequence_id    -- DAT-476
    LEFT OUTER JOIN datawork.mk_clearbankbehavior cbb ON la.Lead_Sequence_ID = cbb.lead_sequence_id  
    left join (SELECT sso_username, min(login_attempt_time) as first_login_time
								 FROM website.mk_clients_sso_login_log
								 where sso_username like '%@%' and user_agent like 'MoneyKey%com.money.key%'
								 group by sso_username) app on la.emailaddress=app.sso_username
	left join webapi.users users on la.lms_customer_id=users.lms_customer_id and users.organization_id = 2 and la.received_time >= @is_epic_rc_start_date and la.lms_code='JAG' 
    left join webapi.web_click wc on la.lms_application_id=wc.loan_header_id
    left JOIN reporting_cf.vcf_lead_source ls on a.lead_provider_id=ls.lead_source_id AND ls.master_source_id=25 
    left join datawork.mk_factortrust_tu ft on la.lead_sequence_id=ft.lead_sequence_id
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
               from reporting_cf.vcf_lms_customer_notifications cn
        		   inner join reporting_cf.vcf_lms_notification_name_mapping nnm on cn.notification_name_id = nnm.notification_name_mapping_id) list
        group by list.customer_id) marketing on la.lms_customer_id=marketing.customer_id
    where
		la.lms_code='JAG'
		-- and la.application_status not in ('Pending (wth New Loan After)', 'voided')-- edited on Aug03,2017
		and la.application_status !='Pending (wth New Loan After)'
		and la.received_time between @std_date and @end_date
		and (la.isuniqueaccept = 1 or (la.loan_sequence=1 and la.isexpress=0))
		and la.IsApplicationTest=0; 
    
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
    SET @process_label ='AFR data population - move to target table', @process_type = 'Insert';
	  -- start transaction;
		-- remove the existing loans, as per business, only keep the latest info for each loan
       DELETE FROM reporting_cf.AFR_Normal  
		  where lms_code = 'JAG' and original_lead_id in (select original_lead_id from reporting_cf.stg_AFR_Normal);
    
    INSERT INTO reporting_cf.AFR_Normal 
    SELECT * FROM reporting_cf.stg_AFR_Normal;
    
    
		-- log the process
		IF sql_code = '00000' THEN
			GET DIAGNOSTICS rowCount = ROW_COUNT;
			SET return_message = CONCAT(@process_type, @success, @total_rows,rowCount);
			CALL reporting.SP_process_log(@valuation_date, @process_name, @process_label, @process_type, return_message, @status_flag_success);
		ELSE
			SET return_message = CONCAT(@process_type, @failed, sql_code, @error_msg ,sql_msg);
			CALL reporting.SP_process_log(@valuation_date, @process_name, @process_label, @process_type, return_message, @status_flag_failure);
		END IF;
   -- COMMIT;
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
    SET @process_label ='AFR data population - Final Step clean up ', @process_type = 'Insert';
		DELETE FROM reporting_cf.AFR_Normal where lms_code = 'JAG' and original_lead_id in ( select original_lead_id from reporting_cf.leads_accepted where lms_code = 'JAG' and IsApplicationTest=1);
			 
			 delete from reporting_cf.AFR_Normal
					where (lms_code, original_lead_id) in 
													(select aa.lms_code, aa.loan_application_id from
														(select lms_code, loan_application_id, count(*) as cnt 
														from reporting_cf.AFR_Normal group by lms_code, loan_application_id
														having cnt>1)aa
														inner join reporting_cf.leads_accepted la 
														on aa.lms_code = la.lms_code and aa.loan_application_id=la.lms_application_id and la.isuniqueaccept!=1); 
				-- Jan 18, 2019 remove DM name if come from external lead provider                   
			update reporting_cf.AFR_Normal
				set dm_name = null
				where provider_name!='Money Key Web';
  
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
 
END;
