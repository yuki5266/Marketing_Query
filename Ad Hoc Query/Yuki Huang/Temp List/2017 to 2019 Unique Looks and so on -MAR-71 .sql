
/*
# of looks
# of unique email addresses
# of accepted leads
lead cost
underwriting cost
# of loans !!

-all by month, starting in January 2017 and continuing up till today. 
-provide the RC external lead info.
*/



	DROP TEMPORARY TABLE IF EXISTS table1;
	CREATE TEMPORARY TABLE IF NOT EXISTS table1 ( INDEX(lead_sequence_id,lead_provider_id,lead_source_id,master_source_id,origination_loan_id) ) 
	AS (

SELECT 
ma.lead_sequence_id,
ma.insert_date,
ma.decision,
ma.decision_detail,
 ma.email,
 ma.lead_cost,
ma.uw_cost,
ma.state,
ma.organization_id,
ma.lead_source_id,
-- la.lead_sequence_id,
-- la.lms_application_id,
la.loan_sequence,
la.lms_code,
la.origination_loan_id,
if(la.origination_loan_id is null,0,1) as IsOriginated,
if(lms.master_source_id=25,0,1) AS Is_External,
if(mam.RC_FROM_LEAD='True',1,0) as Is_RC_External,
lms.master_source_id,
-- mam.RC_FROM_LEAD,
ma.wd_reason ,
IF(ma.wd_reason IS NOT NULL,1,0) AS Is_RAL,
ma.lead_provider_id
-- ls.lead_source_id
from datawork.mk_application ma
left join datawork.mk_application_more mam on ma.lead_sequence_id=mam.lead_sequence_id
JOIN reporting.vmk_lead_source ls on ma.lead_provider_id=ls.lead_source_id and ls.description not in ('MK-EXTERNAL-TEST','MK-INTERNAL-TEST','MKWEB-TEST-BAS', 'MKWEB-TEST-NCP')
inner join jaglms.lead_master_sources lms on ls.master_source_id = lms.master_source_id and lms.organization_id=1
left join reporting.leads_accepted la on ma.lead_sequence_id=la.lead_sequence_id and la.isuniqueaccept=1
where ma.insert_date between '2017-11-01' and '2018-01-01' 
-- and ma.organization_id is null
-- and ma.decision='Accept'

) ;

-- select * from table1;



	DROP TEMPORARY TABLE IF EXISTS table2;
	CREATE TEMPORARY TABLE IF NOT EXISTS table2 
	AS (
SELECT 
year(t1.insert_date) as Received_year,
month(t1.insert_date) as Received_month,
count(*) as Looks_cnt,
sum(if(t1.uw_cost>0 or t1.decision='ACCEPT', 1, 0)) as IsUniqueLook,
count(distinct t1.email) as Unique_emial_cnt,
sum(if(t1.decision='ACCEPT',1,0)) as total_accepted,
sum(if(t1.isOriginated=1,1,0)) as total_loans,
sum(if(t1.loan_sequence=1,1,0)) as 'Is_NC_cnt',
sum(if(t1.loan_sequence=1 and t1.isOriginated=1,1,0)) as 'NC_originated_cnt',
sum(if(t1.loan_sequence>1,1,0)) as 'Is_RC_cnt',
sum(if(t1.loan_sequence>1 and t1.isOriginated=1,1,0)) as 'RC_originated_cnt',
-- sum(if(t1.loan_sequence is null and t1.decision='Accept',1,0)) as Is_Issue_cnt,
sum(if(t1.lead_cost>1 and t1.loan_sequence>1,1,0)) as RC_External,
sum(t1.lead_cost) as total_lead_cost,
sum(t1.uw_cost) as total_uw_cost,
sum(if(t1.Is_External=1,1,0)) as Total_External,
sum(if(t1.Is_RAL=1,1,0)) as Total_RAL
from table1 t1
where t1.Is_External=1
group by month(t1.insert_date)); --  and ma.insert_date <curtime() ;

select * from table2;


select count(*) from datawork.mk_application ma where ma.insert_date>'2019-11-30' and ma.insert_date<'2019-12-01';
















#########################

	DROP TEMPORARY TABLE IF EXISTS table3;
	CREATE TEMPORARY TABLE IF NOT EXISTS table3 
	AS (
  select 
  year(aa.received_time) as Received_year,
  month(aa.received_time) as Received_month,
  sum(aa.isoriginated) as total_originated,
  sum(aa.approved_amount) as total_origination_amount
  from(
  SELECT 
  month(la.received_time),
  la.lms_code, 
  la.lms_customer_id,
  la.lms_application_id, 
  la.loan_number,
  la.state, 
  la.product,
  la.loan_sequence,
  la.application_status
  , la.loan_status, 
  ifnull(la.isoriginated, 
  la.origination_time,
  la.approved_amount,
  la.received_time,
  la.lead_cost as 1_lead_cost,
  la2.lead_cost as 2_lead_cost,
  ifnull(la.lead_cost,la2.lead_cost) as lead_cost,
  la.lead_source_id,
  la.master_source_id,
  la.provider_name,
  la.emailaddress,
  if(la.loan_sequence=1 and ifnull(la.lead_cost,la2.lead_cost)>1,1,0) as NC_External,
  case
  when la.master_source_id=25 then 1
  when la.master_source_id is null then 1
  else 0
  end as Is_Internal
  FROM reporting.leads_accepted la
  left join reporting.leads_accepted la2 on la.lms_customer_id=la2.lms_customer_id and la.lms_code=la2.lms_code and la2.origination_loan_id=if(la.lms_code in('EPIC','TDC'),la.lms_application_id, la.loan_number) -- first record
  where -- la.loan_sequence=1 
  -- and la.lead_cost>1
  la.origination_time is not null
  and date(la.received_time)>='2019-01-01' /*and date(la.received_time)<='2018-12-01'*/) aa
  where aa.Is_Internal=0
  group by year(aa.received_time),month(aa.received_time));
  
  select * from reporting.leads_accepted where emailaddress='marie_yj@yahoo.com';
  
  select * from table3;

  
  
  