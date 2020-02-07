

select report_date,sum(requests) from reporting.sendgrid_statistics group by report_date;
select * from reporting.sendgrid_statistics;



##CF
DROP TEMPORARY TABLE IF EXISTS table1;
CREATE TEMPORARY TABLE IF NOT EXISTS table1  
AS (
select aa.report_date,
aa.List_name,
sum(aa.requests) as total_sent,
sum(aa.Delivered) as Total_Delivered,
(sum(aa.Delivered))/sum(aa.requests) as Delivered_Rate,
sum(aa.unique_opens) as Total_unique_opens,
sum(aa.unique_opens)/sum(aa.Delivered) as Unique_Open_Rate,
sum(aa.unique_clicks) as Total_unique_clicks,
sum(aa.unique_clicks)/sum(aa.unique_opens) AS Unique_CTR,
sum(aa.unsubscribes) as Total_unsubscribes,
sum(aa.bounces) as Total_bounces,
sum(aa.spam_reports) as Total_spam_reports,
sum(aa.blocks) as Total_Blocks
from 
(select 
s.report_date, 
s.category, 
s.requests,
(s.delivered/2) as Delivered, -- the acutual amount delivered
s.unique_opens,
s.unique_clicks,
s.unsubscribes,
s.bounces,
s.spam_reports,
s.blocks,
case 
when s.category in ('CF_AC1','CF_AC2','CF_ACB') then 'AC'
when s.category = 'CF_DDR1' then 'DDR'
when s.category in ('CF_PA1','CF_PA2') then 'PA'
when s.category in ('CF_WA1','CF_WA2','CF_WA3') then 'WAD'
when s.category ='CF_WAB' then 'WAB'
else null
end as List_name
from reporting.sendgrid_statistics s
where s.category like 'cf%' /*and s.report_date>=date_sub(curdate(),interval 14 day)*/) aa
group by aa.List_name,aa.report_date);


-- select * from table1;


DROP TEMPORARY TABLE IF EXISTS table2;
CREATE TEMPORARY TABLE IF NOT EXISTS table2  
AS (
select aa.business_date,aa.List_name_ch,sum(cnt) as total_cnt_ch 
from 
(SELECT 
ifnull(ch.business_date,date(ch.list_generation_time)) as business_date,ch.list_module,
case
when ch.list_module in('ACD','ACH','ACB') then 'AC'
when ch.list_module ='DDR' then 'DDR'
when ch.list_module in ('PA','PA2') then 'PA'
when ch.list_module ='WAD' and ch.key_word in ('WA2','WA10','WA25')then 'WAD'
when ch.list_module ='WAB' then 'WAB'
else null
end as List_name_ch,
ifnull(count(*),0) as cnt
FROM reporting_cf.campaign_history ch
where date(ch.list_generation_time)>='2019-10-01' 
-- where date(ch.list_generation_time)>= date_sub(curdate(),interval 14 day)
group by ch.list_module, ch.business_date) aa
group by aa.List_name_ch,aa.business_date);




-- select * from table2;

DROP TEMPORARY TABLE IF EXISTS table3;
CREATE TEMPORARY TABLE IF NOT EXISTS table3  
AS (
SELECT t1.report_date,
t1.List_name,
t1.total_sent,
t2.List_name_ch,
ifnull(t2.total_cnt_ch,0) as total_cnt_ch,
t1.Total_Delivered,
ifnull(t1.Delivered_Rate,0) as Delivered_Rate,
t1.Total_unique_opens,
ifnull(t1.Unique_Open_Rate,0) as Unique_Open_Rate,
t1.Total_unique_clicks,
ifnull(t1.Unique_CTR,0) as Unique_CTR,
t1.Total_unsubscribes,
t1.Total_bounces,
t1.Total_spam_reports,
t1.Total_Blocks,
if(t1.total_sent between ((t2.total_cnt_ch)-5) and (t2.total_cnt_ch+5),1,0) as IsMatched,
if(t1.report_date=t2.business_date,1,0) as IsRan,
if(t1.total_sent>(ifnull(t2.total_cnt_ch,0)),1,0) as IsMore
FROM table1 t1 
left join table2 t2 on t1.List_name=t2.List_name_ch and t1.report_date=t2.business_date);


select * from table3;







##MK

-- SELECT distinct list_module from reporting.campaign_history;

DROP TEMPORARY TABLE IF EXISTS table1;
CREATE TEMPORARY TABLE IF NOT EXISTS table1  
AS (select 
aa.report_date,
aa.List_name, 
sum(aa.requests) as Total_sent,
sum(aa.Delivered) as Total_Delivered,
sum(aa.Delivered)/sum(aa.requests) as Delivered_Rate,
sum(aa.unique_opens) as Total_unique_opens,
sum(aa.unique_opens)/sum(aa.Delivered) as Unique_Open_Rate,
sum(aa.unique_clicks) as Total_unique_clicks,
sum(aa.unique_clicks)/sum(aa.unique_opens) AS Unique_CTR,
sum(aa.unsubscribes) as Total_unsubscribes,
sum(aa.bounces) as Total_bounces,
sum(aa.spam_reports) as Total_spam_reports,
sum(aa.blocks) as Total_Blocks
from 
(select s.report_date, s.category, s.requests,(s.delivered/2) as Delivered,s.unique_opens,s.unique_clicks,s.unsubscribes,s.spam_reports,s.blocks, s.bounces,
case 
##POL
when s.category like 'MK_POL%' then 'POL'
##PA
when s.category ='MK_PA1_PA2_DM' then 'PADM'
when s.category ='MK_PA1_PA2_NC' then 'PANC'
when s.category ='MK_PA1_PA2_RC' then 'PARC'
##PO
when s.category = 'MK_PO' then 'PO'
when s.category = 'PO MK - CF' then 'PO_CF'
##GC
when s.category ='MK_GC' then 'GC'
when s.category ='MK_GC_LOC' then 'GCLOC'
when s.category = 'OHGC_MK-CF' then 'OHGC'
##RTC
when s.category like 'MK - RTC%' then 'RTC'
##WA
when s.category like 'MK_WA%' then 'WA'
when s.category like 'MK_DMWA%' then 'WADM'
##DDR
when s.category ='MK_DDR_CA_PD' then 'DDR'
when s.category ='MK_DDR_LOC' then 'DDR'
when s.category ='MK_DDR_OH_SEP' then 'DDR'
when s.category ='MK_DDR_SEP' then 'DDR'
else null
end as List_name
from reporting.sendgrid_statistics s
where s.category not like 'cf%' /*and  s.report_date>=date_sub(curdate(),interval 14 day)*/) aa
group by aa.List_name,aa.report_date);


-- SELECT * from table1;

DROP TEMPORARY TABLE IF EXISTS table2;
CREATE TEMPORARY TABLE IF NOT EXISTS table2  
AS (
select aa.business_date,aa.List_name_ch,ifnull(count(aa.business_date),0) as total_cnt from 
(SELECT 
-- ch.business_date,
date(ch.list_generation_time) as business_date,
ch.list_module,
ch.ddr_type,
la.provider_name,
la.campaign_name,
case
when ch.list_module ='AC' then 'AC'
##DDR
when ch.ddr_type ='DDR3' and ch.product='LOC' then 'DDR'
when ch.ddr_type in ('DDR3','DDR9') and ch.product in ('SEP','IPP','FP') then 'DDR'
when ch.ddr_type in ('DDR3','DDR9') and ch.product='PD' and ch.state='CA' then 'DDR'
when ch.ddr_type in ('DDR3','DDR9') and ch.product='SEP' and ch.state='OH' then 'DDR'
##PA
when ch.list_module in ('PA2_RC','PA_RC')  then 'PARC'
when ch.list_module in ('PA', 'PA2') and la.campaign_name is null and la.provider_name is null  then 'PANC'
when ch.list_module in ('PA', 'PA2') and la.campaign_name like '%DM%' and la.provider_name='Money Key Web'  then 'PADM'
##WA
when (ch.list_module ='WAD' and datediff(ch.list_generation_time, ch.withdrawn_time) in (3,10,25) and la.campaign_name like '%DM%' and la.provider_name='Money Key Web')  then 'WADM'
when (ch.list_module ='WAD' and datediff(ch.list_generation_time, ch.withdrawn_time) in (3,10,25) and  la.campaign_name is null and la.provider_name is null)  then 'WA'

##PO
when ch.list_module = 'PO' and ch.product in('SEP','IPP','PD','FP') and ch.state !='OH' then 'PO'
when ch.list_module = 'PO' and ch.product in('SEP','SP') and ch.state ='OH' then 'PO_CF'

##POLNEW
when ch.list_module = 'POL_NEW' and (case when ch.state in('CA','AL') and ch.product='SEP' then 1 else 0 end)=0 and (datediff(ch.list_generation_time, ch.last_repayment_date)) in (7,10,13,15,19,23,26,30,75) then 'POL' -- 2 4 5 6 7 9 10 11 22
when ch.list_module = 'POL_NEW' and (case when ch.state in('CA','AL') and ch.product='SEP' then 1 else 0 end)=0 and (datediff(ch.list_generation_time, ch.last_repayment_date)) = 60 and ch.lms_code='EPIC' then 'POL' -- 19
when ch.list_module = 'POL_NEW' and (case when ch.state in('CA','AL') and ch.product='SEP' then 1 else 0 end)=0 and (datediff(ch.list_generation_time, ch.last_repayment_date)) = 60 and ch.lms_code !='EPIC' then 'POL' -- 20
when ch.list_module = 'POL_NEW' and (case when ch.state ='CA' and ch.product='PD' then 1 else 0 end)=1 and (datediff(ch.list_generation_time, ch.last_repayment_date)) = 45 then 'POL' -- 14
when ch.list_module = 'POL_NEW' and (case when ch.state ='CA' and ch.product='PD' then 1 else 0 end)=1 and (datediff(ch.list_generation_time, ch.last_repayment_date)) = 60 and ch.lms_code ='EPIC'   then 'POL' -- 16
when ch.list_module = 'POL_NEW' and (case when ch.state ='CA' and ch.product='PD' then 1 else 0 end)=1 and (datediff(ch.list_generation_time, ch.last_repayment_date)) = 60 and ch.lms_code !='EPIC'   then 'POL' -- 17
when ch.list_module = 'POL_NEW' and (case when ch.state ='CA' and ch.product='SEP' then 1 else 0 end)=0 and (datediff(ch.list_generation_time, ch.last_repayment_date)) = 3  then 'POL' -- 1 
when ch.list_module = 'POL_NEW' and (case when ch.state in('CA','AL') and ch.product='SEP' then 1 else 0 end)=1 and (datediff(ch.list_generation_time, ch.last_repayment_date)) in (10,20,30,40,50,60,75,90) then 'POL' -- 3 8 12 13 15 18 21 23

else null
end as List_name_ch

FROM reporting.campaign_history ch
LEFT join reporting.leads_accepted la on ch.lms_code = la.lms_code and ch.lms_application_id = la.lms_application_id and la.campaign_name like '%DM%' and la.provider_name='Money Key Web'
where ch.business_date>='2019-10-01' 
-- where date(ch.list_generation_time)>= date_sub(curdate(),interval 14 day)
) aa

group by aa.List_name_ch,aa.business_date);


-- select * from table2;


DROP TEMPORARY TABLE IF EXISTS table3;
CREATE TEMPORARY TABLE IF NOT EXISTS table3  
AS (
select aa.business_date,aa.List_name_ch,IFNULL(sum(cnt),0) as total_cnt from 
(SELECT 
ifnull(ch.list_generation_date,date(ch.insert_datetime)) as business_date,
ch.list_module,
IF(ch.list_module in('TDCLOCGC','JAGLOCGC'),'GCLOC',null) as List_name_ch,
ifnull(count(*),0) as cnt
FROM reporting.loc_gc_campaign_history ch
where ch.list_generation_date>='2019-10-01' 
-- where ch.list_generation_date>= date_sub(curdate(),interval 14 day)
group by ch.list_module, ch.list_generation_date) aa
where List_name_ch is not null
group by aa.List_name_ch,aa.business_date);


-- select * from table3;


DROP TEMPORARY TABLE IF EXISTS table4;
CREATE TEMPORARY TABLE IF NOT EXISTS table4  
AS (
select aa.business_date,aa.List_name_ch,IFNULL(sum(cnt),0) as total_cnt from 
(SELECT 
DATE(ch.list_generation_time) as business_date,
ch.list_module,
case
when ch.list_module='GC' then 'GC'
when ch.list_module='GC_OH' then 'OHGC'
else null
end as List_name_ch,
ifnull(count(*),0) as cnt
FROM reporting.monthly_campaign_history ch
where DATE(ch.list_generation_time)>='2019-10-01' 
-- where DATE(ch.list_generation_time)>= date_sub(curdate(),interval 14 day)
group by ch.list_module, DATE(ch.list_generation_time)) aa
where List_name_ch is not null
group by aa.List_name_ch,aa.business_date);

-- select * from table4;
-- select * from reporting.monthly_campaign_history where list_module='GC_OH' limit 100;




DROP TEMPORARY TABLE IF EXISTS table5;
CREATE TEMPORARY TABLE IF NOT EXISTS table5  
AS (
SELECT 
ch.report_date as business_date,
'RTC' as List_name_ch,
ifnull(count(*),0) as cnt
FROM reporting.rtc_campaign_list ch
where ch.report_date>='2019-10-01' 
-- where ch.report_date>= date_sub(curdate(),interval 14 day) 
group by ch.report_date);

-- select * from table5;
-- select * from reporting.rtc_campaign_list where report_date='2019-10-15';





DROP TEMPORARY TABLE IF EXISTS table6;
CREATE TEMPORARY TABLE IF NOT EXISTS table6  
AS (
SELECT 
t1.report_date,
t1.List_name,
COALESCE(t2.List_name_ch,t3.List_name_ch,t4.List_name_ch,t5.List_name_ch) as List_name_ch,
t1.total_sent,
IFNULL(COALESCE(t2.total_cnt,t3.total_cnt,t4.total_cnt,t5.cnt),0) as Total_cnt_ch,
t1.Total_Delivered,
ifnull(t1.Delivered_Rate,0) as Delivered_Rate,
t1.Total_unique_opens,
ifnull(t1.Unique_Open_Rate,0) as Unique_Open_Rate,
t1.Total_unique_clicks,
ifnull(t1.Unique_CTR,0) as Unique_CTR,
t1.Total_unsubscribes,
t1.Total_bounces,
t1.Total_spam_reports,
t1.Total_Blocks,
if(t1.total_sent between ((COALESCE(t2.total_cnt,t3.total_cnt,t4.total_cnt,t5.cnt))-15) and ((COALESCE(t2.total_cnt,t3.total_cnt,t4.total_cnt,t5.cnt))+15),1,0) as IsMatched,
if(t1.total_sent>(COALESCE(t2.total_cnt,t3.total_cnt,t4.total_cnt,t5.cnt)),1,0) as IsMore
FROM table1 t1 
right join table2 t2 on t1.List_name=t2.List_name_ch and t1.report_date=t2.business_date
right join table3 t3 on t1.List_name=t3.List_name_ch and t1.report_date=t3.business_date
right join table4 t4 on t1.List_name=t4.List_name_ch and t1.report_date=t4.business_date
right join table5 t5 on t1.List_name=t5.List_name_ch and t1.report_date=t5.business_date
);

select * from table6;



select *from table6 WHERE report_date>='2019-10-10' and List_name='POL';












##SP ran multiple time in the same days( Except for ACH)
##cf

select ch.list_module,
count(distinct ch.list_generation_time) as ran_cnt,
max(ch.list_generation_time) as last_gen,
min(ch.list_generation_time) as first_gen,
ch.business_date
from reporting_cf.campaign_history ch 
where ch.list_moduLe != 'ACH'
and ch.business_date>='2019-09-30' 
group by ch.business_date,ch.list_module
having ran_cnt>1;

##mk
select ch.list_module,
count(distinct ch.list_generation_time) as ran_cnt,
max(ch.list_generation_time) as last_gen,
min(ch.list_generation_time) as first_gen,
ch.business_date
from reporting.campaign_history ch 
where ch.list_moduLe != 'ACH'
and date(ch.list_generation_time)>='2019-05-30' 
group by ch.business_date,ch.list_module
having ran_cnt>1;


select * from reporting_cf.campaign_history where list_module='WAD' and business_date=curdate() group by business_date;
