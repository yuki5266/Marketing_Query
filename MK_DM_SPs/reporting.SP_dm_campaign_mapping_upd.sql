DROP PROCEDURE IF EXISTS reporting.SP_dm_campaign_mapping_upd;
CREATE PROCEDURE reporting.`SP_dm_campaign_mapping_upd`()
BEGIN
/*********************************************************************************************************************************
---    NAME : SP_dm_campaign_mapping_upd
---    DESCRIPTION: script for campaign list data population
---    DD/MM/YYYY    By              Comment
---    02/05/2017    Eric Pu         DAT-747 create SP based on the logic and adding exception handling and logs
---																	 DAT-1150 Equifax#2_MEBT didn't get updated
---         												 DAT-1204 correct the update logic and convert historical data                                     
************************************************************************************************************************************/
DECLARE IsHoliday INT DEFAULT 0;
DECLARE NotRunFlag INT DEFAULT 0;
SET SQL_SAFE_UPDATES=0;
SET SESSION tx_isolation='READ-COMMITTED';
SET @start = 'Start', @end = 'End', @success = ' succeeded,', @failed = ' failed, returned SQL_STATE = ', @error_msg = ', error message = ', @total_rows = ' total row count = '; 
SET @process_name = 'SP_dm_campaign_mapping_upd', @status_flag_success = 1, @status_flag_failure = 0;
SET @valuation_date = curdate();
SET @MonthNumber = Month(curdate());
SET @DayNumber = Day(curdate()); 
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
    	-- Declare exception handler for failed insert
    	DECLARE CONTINUE HANDLER FOR SQLEXCEPTION
    		BEGIN
    			GET DIAGNOSTICS CONDITION 1
    				sql_code = RETURNED_SQLSTATE, sql_msg = MESSAGE_TEXT;
    		END;
			SET @process_label ='Main process to populate NC data into campaign_history', @process_type = 'Insert';
			
			truncate table reporting.stg_HB_info;
			insert into stg_HB_info (dm_name,lead_sequence_id, affid, insert_date, hb_application)		
			select dm.dm_name, a.lead_sequence_id, a.affid, a.insert_date, 1 as hb_application
				from datawork.mk_application a
				inner join reporting.dm_campaign_mapping dm on a.affid like dm.affid
																and a.insert_date between date_sub(dm.start_date, interval 5 day) and date_add(dm.expire_date,interval 1 day)
				join jaglms.lead_source ls on ls.lead_source_id = a.lead_provider_id AND ls.master_source_id=25
				where dm.mapping_id not in (49, 50)  and
				dm.expire_date>=date_sub(curdate(), interval 1 day) and
				a.insert_date>=date_sub(curdate(), interval 90 day) 
				group by a.affid
			union -- dat-1150
				select dm.dm_name, a.lead_sequence_id, a.affid, a.insert_date, 1 as hb_application
				from datawork.mk_application a
				inner join reporting.dm_campaign_mapping dm on a.affid like dm.affid
																and a.insert_date between date_sub(dm.start_date, interval 5 day) and date_add(dm.expire_date,interval 1 day)
				join jaglms.lead_source ls on ls.lead_source_id = a.lead_provider_id AND ls.master_source_id=25
				where dm.mapping_id = 49 and a.state !='TX' and
				dm.expire_date>=date_sub(curdate(), interval 1 day) and
				a.insert_date>=date_sub(curdate(), interval 90 day) 
				group by a.affid 
			union 
				select dm.dm_name, a.lead_sequence_id, a.affid, a.insert_date, 1 as hb_application
				from datawork.mk_application a
				inner join reporting.dm_campaign_mapping dm on a.affid like dm.affid
																and a.insert_date between date_sub(dm.start_date, interval 5 day) and date_add(dm.expire_date,interval 1 day)
				join jaglms.lead_source ls on ls.lead_source_id = a.lead_provider_id AND ls.master_source_id=25
				where dm.mapping_id = 50 and a.state ='TX' and
				dm.expire_date>=date_sub(curdate(), interval 1 day) and
				a.insert_date>=date_sub(curdate(), interval 90 day) 
				group by a.affid;

			update reporting.dm_campaign_mapping m inner join (select dm_name, sum(hb_application) as total_application from reporting.stg_HB_info group by dm_name) hb on m.dm_name=hb.dm_name
			set m.total_application= hb.total_application
			where m.expire_date>=date_sub(curdate(), interval 1 day);

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
 
	 -- log the process for completion
		CALL reporting.SP_process_log(@valuation_date, @process_name, @end, null, 'job is done', @status_flag_success);
  END IF;
 
END;
