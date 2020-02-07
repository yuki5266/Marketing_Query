select * from reporting.dm_campaign_mapping;


/*
We need a Year over Year breakdown for MoneyKey DM performance for 2017 vs 2018 vs 2019. Thank you.
?	Total pieces mailed (please note total # of campaigns)
?	Total spend
?	Total Application
?	Total Accepted Leads
?	Conversion
?	Total loan volume
?	Cost Per Funded Loan
?	Cost Per Funded $

*/
        
        
        select id,affid, base_loan_id, organization_id,
        ACCEPT, loan_header_id, loan_amount,
        lead_provider_id, lead_cost, lead_provider_name, 
        lead_source_id decision, decision_detail 
        from datawork.mk_application 
        where insert_date between '2017-03-25' and '2017-5-30' and affid like '%key%' and campaign_name like '%dm%';
        
        
       ###################Validation
       
       select dm.dm_name, a.lead_sequence_id, a.affid, a.insert_date, 1 as hb_application, a.campaign_name,a.state,
       if(a.state in('TX','CA'),1,0) AS TEST_1
				from datawork.mk_application a
				inner join reporting.dm_campaign_mapping dm on a.affid like dm.affid
																and a.insert_date between date_sub(dm.start_date, interval 5 day) and date_add(dm.expire_date,interval 1 day)
				join jaglms.lead_source ls on ls.lead_source_id = a.lead_provider_id AND ls.master_source_id=25
        where   dm.dm_name='Clarity#4-TX&CA' -- in('Clarity#4','Clarity#4-TX&CA') 
        -- a.insert_date>='2019-10-01' -- and a.campaign_name like '%dm%'
				group by a.affid;
      
      
      
      ##########################
      
      
      
      select * from reporting.dm_campaign_mapping where data_provider in ('Equifax','Clarity','Transunion') and start_date>'2019-01-01';
      
      
      select  lead_sequence_id from reporting.leads_accepted where emailaddress='RUBYRAIN@FRONTIER.COM';
      
      
      
      
      
      
      
      DROP TEMPORARY TABLE IF EXISTS table1;
CREATE TEMPORARY TABLE IF NOT EXISTS table1   ( INDEX(affid,start_date,lead_source_id,lead_provider_id,lead_sequence_id,insert_date,expire_date) ) 
AS ( 

select dm.dm_name, 
a.lead_sequence_id, 
a.affid, a.insert_date, 
1 as hb_application, 
a.campaign_name,
a.state,
a.decision, 
a.decision_detail,
dm.start_date,
dm.expire_date,
ls.lead_source_id,
a.lead_provider_id,
dm.total_mail_count,
a.uw_cost,
dm.unit_cost
from datawork.mk_application a
inner join reporting.dm_campaign_mapping dm on a.affid like dm.affid 
and a.insert_date between date_sub(dm.start_date, interval 5 day) and date_add(dm.expire_date,interval 1 day) and dm.data_provider !='FactorTrust' -- and dm.dm_name='Clarity#7'
inner join jaglms.lead_source ls on ls.lead_source_id = a.lead_provider_id AND ls.master_source_id=25
where     
a.insert_date>='2017-01-01' and a.insert_date <'2018-01-20'
and a.affid is not null
group by a.affid);
        
        -- select COUNT(*) from table1;
        
       DROP TEMPORARY TABLE IF EXISTS table2;
CREATE TEMPORARY TABLE IF NOT EXISTS table2   ( INDEX(lead_sequence_id) ) 
AS (   
select t1.*,
if(t1.decision='ACCEPT',1,0) as IsAccepted,
if(la.isoriginated is null,if(la.application_status='Originated',1,0),la.isoriginated) as IsOriginated

from table1 t1
left join reporting.leads_accepted la on t1.lead_sequence_id = la.lead_sequence_id and la.isoriginated=1 and la.loan_sequence=1
);

select COUNT(*) from table2;

       DROP TEMPORARY TABLE IF EXISTS table3;
CREATE TEMPORARY TABLE IF NOT EXISTS table3   ( INDEX(lead_sequence_id) ) 
AS ( 
select t2.*,n.lms_code, n.product, 
n.loan_sequence,n.lead_sequence_id as n_lead_sequence_id, 
n.is_unique_accepts, n.lead_cost,
n.origination_loan_id,
n.original_lead_received_date,
n.application_status, 
n.origination_datetime, n.effective_date, n.originated_loan_amount, 
n.is_1st_payment_defaulted,
n.is_1st_payment_debited

from table2 t2
left join reporting.AFR_Normal n
on t2.lead_sequence_id=n.lead_sequence_id and n.origination_datetime is not null and n.loan_sequence=1

);
       
       
       select * from table3;
       
       select *,count(*) as cnt from table3 group by affid having cnt >1;
       select * from reporting.AFR_Normal where lead_sequence_id=2203978334;
       select * from reporting.leads_accepted where lead_sequence_id=2203978334;
        select * from reporting.leads_accepted where lms_customer_id=669288;

       
       
       DROP TEMPORARY TABLE IF EXISTS table4;
CREATE TEMPORARY TABLE IF NOT EXISTS table4  
AS (  
select t2.dm_name,t2.start_date,t2.total_mail_count,sum(t2.uw_cost) as Total_UW_Cost,
count(*) as Total_applied,
sum(t2.IsAccepted) as Total_accepted,
sum(t2.IsOriginated) as Total_originated,
sum(t2.is_1st_payment_debited) as 1st_payment_debited_cnt,
sum(ifnull(t2.is_1st_payment_defaulted,0)) as FPD_cnt,
sum(t2.originated_loan_amount) as Total_loan_amount,
t2.unit_cost,
year(t2.start_date) as 'Year'
from table3 t2 
group by t2.dm_name

);

select * from table4;




##################
       DROP TEMPORARY TABLE IF EXISTS table5;
CREATE TEMPORARY TABLE IF NOT EXISTS table5  
AS (
select 
t2.dm_name,
t2.dm_name1,
t2.start_date,
sum(t2.total_mail_count) as total_mail_count,
t2.unit_cost,
sum(t2.uw_cost) as Total_UW_Cost,
sum(t2.Total_applied) as Total_applied,
sum(t2.IsAccepted) as Total_accepted,
sum(t2.IsOriginated) as Total_originated,
sum(t2.is_1st_payment_debited) as 1st_payment_debited_cnt,
sum(ifnull(t2.is_1st_payment_defaulted,0)) as FPD_cnt,
sum(t2.originated_loan_amount) as Total_loan_amount,
t2.`Year`
from(
select 
aa.dm_name,
case 
when aa.dm_name in ('TU#5_MKN','TU#5_MNK') then 'TU#5'
when aa.dm_name in ('TU#5_MKN-Remail','TU#5_MNK-Remail') then 'TU#5_Remail'
when aa.dm_name in ('TU#6_MNS','TU#6_MSN') then 'TU#6'
when aa.dm_name in ('TU#7_MKR','TU#7_MRK') then 'TU#7'
when aa.dm_name in ('TU#8_MKT','TU#8_MTK') then 'TU#8'
when aa.dm_name in ('TU#8_MRKT_Remail','TU#8_MRTK_Remail') then 'TU#8_Remail'
when aa.dm_name in ('Clarity#10_MKS','Clarity#10_MSK') then 'Clarity#10'
when aa.dm_name in ('Clarity#9_MNP','Clarity#9_MPN') then 'Clarity#9'
when aa.dm_name in ('TU#1-CA','TU#1-OH&TX') then 'TU#1'
when aa.dm_name in ('Clarity#4-TX&CA','Clarity#4') then 'Clarity#4'
else aa.dm_name
end as dm_name1,
aa.start_date,
aa.total_mail_count,
aa.unit_cost,
sum(aa.uw_cost) as uw_cost,
count(*) as Total_applied,
sum(aa.IsAccepted) as IsAccepted,
sum(aa.IsOriginated) as IsOriginated,
sum(aa.is_1st_payment_debited) as is_1st_payment_debited,
sum(aa.is_1st_payment_defaulted) as is_1st_payment_defaulted,
sum(aa.originated_loan_amount) as originated_loan_amount,
year(aa.start_date) as 'Year'
from YOY_DM_Performance aa
group by aa.dm_name ) t2
group by t2.dm_name1);

-- select * from  YOY_DM_Performance limit 100;
select * from table5;


select aa.*,case 
when aa.dm_name in ('TU#5_MKN','TU#5_MNK') then 'TU#5'
when aa.dm_name in ('TU#5_MKN-Remail','TU#5_MNK-Remail') then 'TU#5_Remail'
when aa.dm_name in ('TU#6_MNS','TU#6_MSN') then 'TU#6'
when aa.dm_name in ('TU#7_MKR','TU#7_MRK') then 'TU#7'
when aa.dm_name in ('TU#8_MKT','TU#8_MTK') then 'TU#8'
when aa.dm_name in ('TU#8_MRKT_Remail','TU#8_MRTK_Remail') then 'TU#8_Remail'
when aa.dm_name in ('Clarity#10_MKS','Clarity#10_MSK') then 'Clarity#10'
when aa.dm_name in ('Clarity#9_MNP','Clarity#9_MPN') then 'Clarity#9'
when aa.dm_name in ('TU#1-CA','TU#1-OH&TX') then 'TU#1'
when aa.dm_name in ('Clarity#4-TX&CA','Clarity#4') then 'Clarity#4'
else aa.dm_name
end as dm_name1 from YOY_DM_Performance aa;



       DROP TEMPORARY TABLE IF EXISTS table6;
CREATE TEMPORARY TABLE IF NOT EXISTS table6  
AS (
select 
dm_name1,
start_date,
total_mail_count,
Total_applied,
Total_accepted,
Total_applied/total_mail_count as `Net Response Rate%`,
Total_accepted/total_mail_count as  `Accepted Response Rate%`,
Total_originated,
1st_payment_debited_cnt,
FPD_cnt,
Total_originated/Total_accepted as `Conversion%`,
FPD_cnt/Total_originated as `FPD%`,
Total_loan_amount,
unit_cost,
Total_UW_Cost,
unit_cost*total_mail_count as `Cost Estimate`,
(unit_cost*total_mail_count)/Total_originated as `CPF`,
((unit_cost*total_mail_count)+Total_UW_Cost)/Total_originated as CPF_with_UW_cost,
Year(start_date) as `Year`

from table5);

select * from table6;




       DROP TEMPORARY TABLE IF EXISTS table7;
CREATE TEMPORARY TABLE IF NOT EXISTS table7  
AS (
select 
`Year`,
Count(dm_name1) as Campaign_cnt,
sum(total_mail_count) as Total_Mail,
SUM(Total_applied) as Total_Applied,
sum(Total_accepted) as Total_Accepted,
AVG(`Net Response Rate%`) AS `Net Response Rate%` ,
AVG(`Accepted Response Rate%`) AS `Accepted Response Rate%` ,
SUM(Total_originated) as Total_Originated,
sum(1st_payment_debited_cnt) as Total_1st_payment_debited_cnt,
sum(FPD_cnt) as Total_FPD_cnt,
AVG(`Conversion%`) AS `Conversion%`,
avg( `FPD%`) as `FPD%`,
SUM(Total_UW_Cost) as UW_Cost,
sum(`Cost Estimate`) as `Cost Estimate`,
AVG(`CPF`) AS `CPF`,
AVG(CPF_with_UW_cost) AS CPF_with_UW_cost,
(sum(`Cost Estimate`) /sum(total_mail_count)) AS Average_unit_cost
from table6 group by `Year`);

select * from table7;



select * from YOY_DM_Performance where dm_name='Clarity#6-Remail';

select * from reporting.leads_accepted where lead_sequence_id=2196475456;
select * from reporting.AFR_Normal where lead_sequence_id=2196475456;


UPDATE YOY_DM_Performance
SET is_1st_payment_debited=0
where lead_sequence_id in(2195240175,2196475456);