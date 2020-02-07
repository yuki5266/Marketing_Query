-- truncate table reporting.stg_HB_info;
		    -- insert into stg_HB_info (dm_name,lead_sequence_id, affid, insert_date, hb_application)
        
        
			select 
      dm.dm_name, 
      a.lead_sequence_id, 
      a.affid, 
      a.insert_date, 
      1 as hb_application
			from datawork.mk_application a
			inner join reporting.dm_campaign_mapping dm on a.affid like dm.affid and a.insert_date between date_sub(dm.start_date, interval 5 day) and dm.expire_date
			join jaglms.lead_source ls on ls.lead_source_id = a.lead_provider_id AND ls.master_source_id=25
			where dm.mapping_id <> 50 and dm.expire_date>=date_sub(curdate(), interval 1 day) and a.insert_date>=date_sub(curdate(), interval 90 day) 
			group by a.affid
      
      
		union -- dat-1150
    
    
      select dm.dm_name, a.lead_sequence_id, a.affid, a.insert_date, 1 as hb_application
			from datawork.mk_application a
			inner join reporting.dm_campaign_mapping dm on a.affid like dm.affid
														  and a.insert_date between date_sub(dm.start_date, interval 5 day) and dm.expire_date
			join jaglms.lead_source ls on ls.lead_source_id = a.lead_provider_id AND ls.master_source_id=25
			where dm.mapping_id = 50 and
			dm.expire_date>=date_sub(curdate(), interval 1 day) and
			a.insert_date>=date_sub(curdate(), interval 90 day) 
			group by a.affid;

			-- update 
      
      select * from reporting.dm_campaign_mapping m inner join (select dm_name, sum(hb_application) as total_application from reporting.stg_HB_info group by dm_name) hb on m.dm_name=hb.dm_name
			-- set m.total_application= hb.total_application
			where m.expire_date>=date_sub(curdate(), interval 1 day);



select * from reporting.dm_campaign_mapping;

select affid,state, customer_id from datawork.mk_application where insert_date>='2019-10-01' and affid like 'MEB%' and state='TX';
select affid,state, customer_id from datawork.mk_application where insert_date>='2019-10-01' and affid like 'MEB%' and state!='TX';


select state,decision from datawork.mk_application where affid in('Mebt2c5s8','Mebt6x9q9','MEBT8Z3Y6','MEBT7T3R7','MEBT9Z3L8');







show processlist;


#######################

DROP TEMPORARY TABLE IF EXISTS table1;
CREATE TEMPORARY TABLE IF NOT EXISTS table1  
AS (
select 
      dm.dm_name, 
      a.lead_sequence_id, 
      a.affid, 
      a.insert_date, 
      1 as hb_application
			from datawork.mk_application a
			inner join reporting.dm_campaign_mapping dm on a.affid like dm.affid and a.insert_date between date_sub(dm.start_date, interval 5 day) and dm.expire_date
			join jaglms.lead_source ls on ls.lead_source_id = a.lead_provider_id AND ls.master_source_id=25
			where dm.mapping_id <> 50 and dm.expire_date>=date_sub(curdate(), interval 1 day) and a.insert_date>=date_sub(curdate(), interval 90 day) 
			group by a.affid);


select * from table1 where affid not in (select affid from datawork.mk_application where insert_date>='2019-10-01' and affid like 'MEB%' and state='TX');