/*
Request:
We had a request from Product to pause CA-PD DDR back in August 16, 2019 in an effort to lower defaults. 
When we automated Phase 1 of MoneyKey emails that included DDR, PA & WA it turned CA-PD DDR back on. 

Can you do an analysis comparing 1st payment default % from the following time frames (2 week evaluation period for both scenarios):

1.August 16 to September 3rd (CA-PD DDR was turned off) -- Due day on from August.13rd.2019 to September.9th.2019 will not receive any DDR

2.September 16 to September 30 (CA-PD DDR was live via automation)

##Automated CA PD query
##CA PD
SELECT 
Customer_FirstName as FirstName,
email,
origination_loan_id as loan_id, 
CONCAT('$',round(ach_debit,2)) as ach_debit_amount , 
date_Format(ach_date, '%M %e, %Y') as ach_debit_date 
FROM reporting.campaign_history
where date(list_generation_time)=curdate()  and list_module='DDR'
and SUBSTR(SUBSTR(email, INSTR(email, '@'), INSTR(email, '.')), 2) not in ('epic.lmsmail.com', 'moneykey.com')
and ddr_type in ('DDR3', 'DDR9')
and state ='CA'
and product='PD';




*/

####################  AFR FPD%
/*
	DROP TEMPORARY TABLE IF EXISTS table1;
		CREATE TEMPORARY TABLE IF NOT EXISTS table1 ( INDEX(base_loan_id) ) as 
		(
   
    (SELECT 
    ps.base_loan_id,
    ps.customer_id, 
    min(psi.item_date) as first_debit_date, 
    psi.status  
			FROM jaglms.lms_base_loans bl 
      inner join jaglms.lms_payment_schedules ps on bl.base_loan_id = ps.base_loan_id
			INNER JOIN jaglms.lms_payment_schedule_items psi ON ps.payment_schedule_id = psi.payment_schedule_id
			WHERE bl.origination_date >= subdate('2019-09-30',180) 
      and psi.total_amount > 0
			and psi.status in ('Return', 'Missed')  
			group by ps.base_loan_id)) ;
        
		-- update reporting.AFR_Normal n 
			inner join tmp_1st_debit_date aa on n.origination_loan_id=aa.base_loan_id
			set n.is_1st_payment_debited = 1, 
      n.1st_payment_debit_date = aa.first_debit_date,
					n.is_1st_payment_defaulted = if(aa.status in ('Return', 'Missed'), 1, 0)
			where n.lms_code='JAG' and n.provider_name !='Money Key Web' 
				and n.origination_datetime >= subdate(curdate(),180)
				and n.nc_lms_accepts=1
				and n.is_1st_payment_debited=0;



*/


############


/*
DROP TEMPORARY TABLE IF EXISTS table1;
		CREATE TEMPORARY TABLE IF NOT EXISTS table1 as
    (
    select
    n.lms_code,
    n.product, 
    n.state, 
    n.loan_sequence,
    n.customer_id,
    n.loan_application_id, 
    n.origination_loan_id, 
    n.pay_frequency, 
    n.is_originated, 
    n.application_status, 
    n.origination_datetime,
    n.originated_loan_amount, 
    n.`1st_payment_due_date`, 
    n.is_1st_payment_debited, 
    n.`1st_payment_debit_date`,
    n.is_1st_payment_defaulted,
    -- n.principal_paid,
    -- n.fees_paid,
    ps.base_loan_id,
    min(psi.item_date) as first_debit_date,
    psi.status,
    psi.total_amount,
    psi.amount_fee,
    psi.amount_int, 
    psi.amount_prin,
    psi.item_type
    from reporting.AFR_Normal n
    left join jaglms.lms_base_loans bl on n.origination_loan_id=bl.base_loan_id
    left join jaglms.lms_payment_schedules ps on bl.base_loan_id = ps.base_loan_id
		left JOIN jaglms.lms_payment_schedule_items psi ON ps.payment_schedule_id = psi.payment_schedule_id  and psi.total_amount > 0 and psi.status in ('Return', 'Missed')
    where n.state='CA' and n.product='PD' and n.origination_datetime>0 and n.`1st_payment_debit_date` between '2019-08-13' and '2019-09-09'
    group by ps.base_loan_id);
    
*/

#####################
-- August 16 to September 3rd (CA-PD DDR was turned off) -- Due day on from August.21st.2019 to September.16th.2019 will not receive any DDR
DROP TEMPORARY TABLE IF EXISTS table1;
		CREATE TEMPORARY TABLE IF NOT EXISTS table1 as
    (
    select * from (
    select
la.lms_code, la.lms_customer_id, la.loan_number, 
la.state, la.product, la.pay_frequency, la.application_status, 
la.loan_status, la.original_lead_id, la.origination_loan_id,
la.isoriginated, la.origination_time, la.effective_date, 
la.emailaddress, 
    ps.base_loan_id,
    min(psi.item_date) as first_debit_date,
    psi.status,
    psi.total_amount,
    psi.amount_fee,
    psi.amount_int, 
    psi.amount_prin,
    psi.item_type,
    if(psi.status in ('Return', 'Missed'),1,0) as Is_FPD
    from reporting.leads_accepted la
    left join jaglms.lms_base_loans bl on la.loan_number =bl.base_loan_id
    left join jaglms.lms_payment_schedules ps on bl.base_loan_id = ps.base_loan_id
		left JOIN jaglms.lms_payment_schedule_items psi ON ps.payment_schedule_id = psi.payment_schedule_id and psi.total_amount > 0  and psi.status in ('Return', 'Missed', 'Cleared','Correction')  
    where la.state='CA' and la.product='PD' and la.origination_time>0 and date(la.origination_time) between '2019-05-01' and '2019-09-17' 
    group by ps.base_loan_id) aa
    where aa.first_debit_date between '2019-08-21' and '2019-09-16');

select * from table1;



#####################
--- September 16 to Oct.02 (CA-PD DDR was live via automation) 9.19 to 10.15

DROP TEMPORARY TABLE IF EXISTS table2;
		CREATE TEMPORARY TABLE IF NOT EXISTS table2 as
    (
    select * from (
    select
la.lms_code, la.lms_customer_id, la.loan_number, 
la.state, la.product, la.pay_frequency, la.application_status, 
la.loan_status, la.original_lead_id, 
la.isoriginated, la.origination_time, la.effective_date, 
la.emailaddress, 
    ps.base_loan_id,
    min(psi.item_date) as first_debit_date,
    psi.status,
    psi.total_amount,
    psi.amount_fee,
    psi.amount_int, 
    psi.amount_prin,
    psi.item_type,
    if(psi.status in ('Return', 'Missed'),1,0) as Is_FPD
    from reporting.leads_accepted la
    left join jaglms.lms_base_loans bl on la.loan_number =bl.base_loan_id
    left join jaglms.lms_payment_schedules ps on bl.base_loan_id = ps.base_loan_id
		left JOIN jaglms.lms_payment_schedule_items psi ON ps.payment_schedule_id = psi.payment_schedule_id and psi.total_amount > 0  and psi.status in ('Return', 'Missed', 'Cleared','Correction')  
    where la.state='CA' and la.product='PD' and la.origination_time>0 and date(la.origination_time) between '2019-07-01' and '2019-10-15' 
    group by ps.base_loan_id) aa
    where aa.first_debit_date between '2019-09-19' and '2019-10-15');


-- select * from table2;


DROP TEMPORARY TABLE IF EXISTS table3;
		CREATE TEMPORARY TABLE IF NOT EXISTS table3 as
    (
SELECT 
ch.business_date, ch.list_id, ch.list_name, 
ch.lms_customer_id, ch.lms_application_id, 
ch.lms_code,ch.origination_loan_id, ch.email,ch.ddr_type,
count(ch.list_generation_time) as cnt
FROM reporting.campaign_history ch
where date(ch.list_generation_time) between '2019-09-01' and '2019-10-31'  
and ch.list_module='DDR'
and SUBSTR(SUBSTR(ch.email, INSTR(ch.email, '@'), INSTR(ch.email, '.')), 2) not in ('epic.lmsmail.com', 'moneykey.com')
and ch.ddr_type in ('DDR3', 'DDR9')
and ch.state ='CA'
and ch.product='PD'
group by ch.origination_loan_id, ch.lms_code);


-- select * from table3;

DROP TEMPORARY TABLE IF EXISTS table4;
		CREATE TEMPORARY TABLE IF NOT EXISTS table4 as
    (
select
t2.lms_code,
t2.lms_customer_id,
t2.loan_number,
t2.state,
t2.product,
t2.pay_frequency,
t2.loan_status,
t2.original_lead_id,
t2.isoriginated,
t2.origination_time,
t2.effective_date,
t2.emailaddress,
t2.base_loan_id,
t2.first_debit_date,
t2.status,
t2.total_amount,
t2.amount_fee,
t2.amount_prin,
t2.Is_FPD,
t3.origination_loan_id,
t3.ddr_type,
t3.cnt
from table2 t2 
left join table3 t3 on t3.lms_code=t2.lms_code and t3.origination_loan_id=t2.loan_number);

select * from table4;



/*
select * from reporting.campaign_history ch where ch.email='maritesromero@icloud.com' and ch.list_module='DDR';

select distinct business_date from reporting.campaign_history ch where  ch.list_module='DDR' and ch.business_date>'2019-08-01';


#############Validate
select * from jaglms.lms_payment_schedule_items where payment_schedule_id in
(select payment_schedule_id from jaglms.lms_payment_schedules where base_loan_id =1104690);

select * from reporting.leads_accepted where emailaddress='dcgridiron65@gmail.com';*/




################################Combined


DROP TEMPORARY TABLE IF EXISTS table5;
		CREATE TEMPORARY TABLE IF NOT EXISTS table5 ( index(base_loan_id,loan_number))as
    (
    select * from (
    select
la.lms_code, la.lms_customer_id, la.loan_number, 
la.state, la.product, la.pay_frequency, la.application_status, 
la.loan_status, la.original_lead_id, 
la.isoriginated, la.origination_time, la.effective_date, 
la.emailaddress, 
    ps.base_loan_id,
    min(psi.item_date) as first_debit_date,
    psi.status,
    psi.total_amount,
    psi.amount_fee,
    psi.amount_int, 
    psi.amount_prin,
    psi.item_type,
    if(psi.status in ('Return', 'Missed'),1,0) as Is_FPD
    from reporting.leads_accepted la
    left join jaglms.lms_base_loans bl on la.loan_number =bl.base_loan_id
    left join jaglms.lms_payment_schedules ps on bl.base_loan_id = ps.base_loan_id
		left JOIN jaglms.lms_payment_schedule_items psi ON ps.payment_schedule_id = psi.payment_schedule_id and psi.total_amount > 0  and psi.status in ('Return', 'Missed', 'Cleared','Correction')  
    where la.state='CA' and la.product='PD' and la.origination_time>0 and date(la.origination_time) between '2019-05-01' and '2019-12-01' 
    group by ps.base_loan_id) aa
    where aa.first_debit_date between '2019-08-21' and '2019-10-14');


-- select * from table5;


DROP TEMPORARY TABLE IF EXISTS table6;
		CREATE TEMPORARY TABLE IF NOT EXISTS table6 as
    (
SELECT 
ch.business_date, ch.list_id, ch.list_name, 
ch.lms_customer_id, ch.lms_application_id, 
ch.lms_code,ch.origination_loan_id, ch.email,ch.ddr_type,
count(ch.list_generation_time) as cnt,
max(ch.ach_date) as debit_day1,
min(ch.ach_date) as debit_day2
FROM reporting.campaign_history ch
where( date(ch.list_generation_time) between '2019-09-04' and '2019-12-01' or date(ch.list_generation_time) between '2019-08-08' and '2019-8-15')
and ch.list_module='DDR'
and SUBSTR(SUBSTR(ch.email, INSTR(ch.email, '@'), INSTR(ch.email, '.')), 2) not in ('epic.lmsmail.com', 'moneykey.com')
and ch.ddr_type in ('DDR3', 'DDR9')
and ch.state ='CA'
and ch.product='PD'
group by ch.origination_loan_id, ch.lms_code);


-- select * from table6;

DROP TEMPORARY TABLE IF EXISTS table7;
		CREATE TEMPORARY TABLE IF NOT EXISTS table7 ( index(origination_loan_id,loan_number))as
    (
select
t2.lms_code,
t2.lms_customer_id,
t2.loan_number,
t2.state,
t2.product,
t2.pay_frequency,
t2.loan_status,
t2.original_lead_id,
t2.isoriginated,
t2.origination_time,
t2.effective_date,
t2.emailaddress,
t2.base_loan_id,
t2.first_debit_date,
datediff(t2.first_debit_date,t2.effective_date)as debit_days_since_originated,
t3.debit_day1,
t3.debit_day2,
case
when t2.first_debit_date between '2019-8-21' and '2019-09-06' then 'Period1'
when t2.first_debit_date between '2019-9-18' and '2019-10-04' then 'Period2'
else 'Else'
end as Period_filter,
t2.status,
t2.total_amount,
t2.amount_fee,
t2.amount_prin,
t2.Is_FPD,
t3.origination_loan_id,
t3.ddr_type,
t3.cnt,
if(t3.ddr_type is null,0,1) as Is_Received_DDR
from table5 t2 
left join table6 t3 on t3.lms_code=t2.lms_code and t3.origination_loan_id=t2.loan_number);

select * from table7;


SELECT 
count(*)
FROM reporting.campaign_history ch
where date(ch.list_generation_time) between '2019-08-16' and '2019-09-03'  
and ch.list_module='DDR'
-- and ch.ddr_type in ('DDR3', 'DDR9')
and ch.state ='CA'
and ch.product='PD';



