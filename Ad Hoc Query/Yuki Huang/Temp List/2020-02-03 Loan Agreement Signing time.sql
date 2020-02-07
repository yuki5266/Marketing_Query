

select * from  webapi.tracking order by `timestamp` desc limit 1000;


select * from  webapi.tracking where user_name='minajohnson07@gmail.com';
select * from jaglms.lms_loan_header where lead_sequence_id=2328373124;


select * from reporting.leads_accepted where lms_customer_id=998048;


select insert_date, decision, decision_detail from datawork.mk_application where email='rcldpierre@yahoo.com';

     
####################New version
      
      

## Find out the first view and submit time

	DROP TEMPORARY TABLE IF EXISTS table1;
			CREATE TEMPORARY TABLE IF NOT EXISTS table1 
			AS (

 select aa.*,
      min(aa.timestamp) as start_time,
      max(aa.timestamp) as end_time from (

select
t.`timestamp`, t.page, t.description, t.session_id, t.user_name, 
t.lms_customer_id, t.lead_sequence_id, t.isExternal, t.organization_id,
n.state,
n.decision,
n.decision_detail,
case

when t.page in('loan_docs-CI') then 'CI'
when t.page in('loan_docs_submit-CI') then 'CI_Submit'

when t.page in('loan_docs-CSD') then 'CSD'
when t.page in('loan_docs_submit-CSD') then 'CSD_Submit'

when t.page in('loan_docs-CSA') then 'CSA'
when t.page in('loan_docs_submit-CSA') then 'CSA_Submit'

when t.page in('loan_docs-PP') then 'PP'
when t.page in('loan_docs_submit-PP') then 'PP_Submit'

when t.page in('loan_docs-LA') then 'LA'
when t.page in('loan_docs_submit-LA') then 'LA_Submit'

when t.page in('loan_docs-ACH') then 'ACH'
when t.page in('loan_docs_submit-ACH') then 'ACH_Submit'


when t.page in('loan_docs-TC') then 'TC'
when t.page in('loan_docs_submit-TC') then 'TC_Submit'

when t.page in('loan_docs-TILA') then 'TILA'
when t.page in('loan_docs_submit-TILA') then 'TILA_Submit'

when t.page in('loan_docs-ARB') then 'ARB'
when t.page in('loan_docs_submit-ARB') then 'ARB_Submit'

else null
end as Group_filter
from webapi.tracking t
join datawork.mk_application n on t.lead_sequence_id=n.lead_sequence_id
where 
n.insert_date BETWEEN '2020-01-01' AND '2020-02-04'
and  t.page like '%loan_docs%' 
and t.organization_id=1) aa
where aa.Group_filter is not null
group by aa.lead_sequence_id,aa.Group_filter,aa.user_name,aa.session_id)
;



-- select * from table1;


DROP TEMPORARY TABLE IF EXISTS table2;
			CREATE TEMPORARY TABLE IF NOT EXISTS table2 
			AS (
      select    aa.*,
      min(aa.start_time) as Start_time1,
      max(aa.start_time) as End_time1,
      timediff(max(aa.start_time),min(aa.start_time)) as time_spent_on_page
      from(
      select
 t1.*,
      case 
      when t1.Group_filter in('CI','CI_Submit') then 'CI1'
      when t1.Group_filter in('CSD','CSD_Submit') then 'CSD1'
      when t1.Group_filter in('CSA','CSA_Submit') then 'CSA1'
      when t1.Group_filter in('PP','PP_Submit') then 'PP1'
      when t1.Group_filter in('LA','LA_Submit') then 'LA1'
      when t1.Group_filter in('ACH','ACH_Submit') then 'ACH'
      when t1.Group_filter in('TC','TC_Submit') then 'TC1'
      when t1.Group_filter in('TILA','TILA_Submit') then 'TILA1'
      when t1.Group_filter in('ARB','ARB_Submit') then 'ARB1'
      ELSE null
      end as Group_filter1     
      from table1 t1) aa
      group by aa.Group_filter1,aa.lead_sequence_id,aa.user_name,aa.session_id);

-- select * from table2;


DROP TEMPORARY TABLE IF EXISTS table3;
			CREATE TEMPORARY TABLE IF NOT EXISTS table3
			AS (
SELECT 
t2.user_name,
t2.lead_sequence_id,
min(t2.Start_time1) as Start_time2,
t2.Group_filter
FROM table2 t2
group by t2.user_name,t2.lead_sequence_id,t2.Group_filter1);

-- select * from table3;


-- Table3: breakdown by pages, include NC and RC
DROP TEMPORARY TABLE IF EXISTS table4;
			CREATE TEMPORARY TABLE IF NOT EXISTS table4(index(Start_time1,Start_time2,lead_sequence_id,Group_filter))
			AS (
      select 
      t2.session_id,
      t2.user_name,
      t2.lms_customer_id,
      t2.lead_sequence_id,
      t2.organization_id,
      t2.state,
      t2.Group_filter1,
      t2.Start_time1,
      t2.End_time1,
      t2.time_spent_on_page,
      t3.Start_time2,
      time_to_sec(time_spent_on_page) as Seconds_spent,
      (time_to_sec(time_spent_on_page)/60) as Minutes_spent,
      t2.Group_filter
      
      from table2 t2  
      inner join table3 t3 on t2.Start_time1=t3.Start_time2 and t2.lead_sequence_id=t3.lead_sequence_id and t2.Group_filter=t3.Group_filter);
      
     -- select * from table4; 
      
   -- TABLE5: to see the signing time for NC and RC   
  DROP TEMPORARY TABLE IF EXISTS table5;
			CREATE TEMPORARY TABLE IF NOT EXISTS table5(index(lead_sequence_id))
			AS (    
      select 
      t4.*, sum(t4.Seconds_spent) as total_second_spent,sum(t4.Minutes_spent) as totl_minutes_spent,  n.nc_lms_accepts, n.rc_lms_accepts, n.is_returning, n.loan_sequence,  n.loan_application_id, n.product, n.lms_code, n.is_originated,
      if(n.loan_sequence=1,1,0) as Is_NC
      from table4 t4
      left join reporting.z_AFR_Normal n on t4.lead_sequence_id=n.lead_sequence_id
      group by t4.lead_sequence_id);
      
      select * from table5;
      
      

select * from  webapi.tracking where user_name='patricemclendon45@gmail.com';
select * from reporting.leads_accepted where lms_customer_id='1196638';






































###############IGNORE



##? pages
			DROP TEMPORARY TABLE IF EXISTS table2;
			CREATE TEMPORARY TABLE IF NOT EXISTS table2 
			AS (
      select aa.*,
      min(aa.timestamp) as start_time,
      max(aa.timestamp) as end_time,
      timediff(max(aa.timestamp),min(aa.timestamp)) as page_duration_time
      
      from (
select
t.`timestamp`, t.page, t.description, t.session_id, t.user_name, 
t.lms_customer_id, t.lead_sequence_id, t.isExternal, t.organization_id,
case
##6 PAGES TX
when t.page in('loan_docs-CI','loan_docs_submit-CI'/*,'loan_docs_submit-ro-CI'*/) then 'CI'
when t.page in('loan_docs-CSD','loan_docs_submit-CSD'/*,'loan_docs_submit-ro-CSD'*/) then 'CSD'
when t.page in('loan_docs-CSA','loan_docs_submit-CSA'/*,'loan_docs_submit-ro-CSA'*/) then 'CSA'
when t.page in('loan_docs-PP','loan_docs_submit-PP'/*,'loan_docs_submit-ro-PP'*/) then 'PP'
when t.page in('loan_docs-LA','loan_docs_submit-LA'/*,'loan_docs_submit-ro-LA'*/) then 'LA'
when t.page in('loan_docs-ACH','loan_docs_submit-ACH') then 'ACH'

##5 PAGES TN KS
-- when t.page in('loan_docs-ACH','loan_docs_submit-ACH') then 'ACH'
-- when t.page in('loan_docs-CI','loan_docs_submit-CI','loan_docs_submit-ro-CI') then 'CI'
when t.page in('loan_docs-TC','loan_docs_submit-TC'/*,'loan_docs_submit-ro-TC'*/) then 'TC'
when t.page in('loan_docs-TILA','loan_docs_submit-TILA'/*,'loan_docs_submit-ro-TILA'*/) then 'TILA'
when t.page in('loan_docs-ARB','loan_docs_submit-ARB'/*,'loan_docs_submit-ro-ARB'*/) then 'ARB'

##4 PAGES ('MS','MO','ID','WI','DE','CA','SC','UT','NM','AL')
-- when t.page in('loan_docs-ACH','loan_docs_submit-ACH') then 'ACH'
-- when t.page in('loan_docs-CI','loan_docs_submit-CI','loan_docs_submit-ro-CI') then 'CI'
-- when t.page in('loan_docs-ARB','loan_docs_submit-ARB','loan_docs_submit-ro-ARB') then 'ARB'
-- when t.page in('loan_docs-LA','loan_docs_submit-LA','loan_docs_submit-ro-LA') then 'LA'

else null
end as Group_filter,
n.state,
n.decision,
n.decision_detail,
lh.created_date
from webapi.tracking t
join datawork.mk_application n on t.lead_sequence_id=n.lead_sequence_id
left join jaglms.lms_loan_header lh on lh.lead_sequence_id=n.lead_sequence_id
where 
n.insert_date>='2020-02-01'
and  t.page like '%loan_docs%' 
and t.organization_id=1
) aa
where aa.Group_filter is not null
group by aa.lead_sequence_id,aa.Group_filter,aa.user_name,aa.session_id
);


select * from table2 where user_name='rcldpierre@yahoo.com';
