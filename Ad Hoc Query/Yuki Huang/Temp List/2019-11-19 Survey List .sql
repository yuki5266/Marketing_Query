/*
List1: SEP Customers who have paid off loan L1, and taken L2 in the last 30 days (L2:Originated or just has applied for second loan?)
List 2: SEP Customers who have paid off loan L1 in the last 30 days, but did not take L2 --> should be 30 to 60 days who has already received 3 POL email (never re-apply)
List 3: SEP Customers who have paid off L1, and requested a second loan through a loan provider in the last 30 days (but had at least 30 days between L1 & L2) (L2:Originated or just has applied for second loan?)


*/



##List1&3
##JAG_PAIDOFF


select 
la2.lms_code, 
la2.lms_customer_id, 
la2.state,
la2.product,
la2.emailaddress,
la2.customer_firstname, 
la2.customer_lastname,
la2.lms_application_id as L2_lms_application_id,
la2.loan_number AS L2_loan_number,
la2.loan_sequence, 
la2.application_status as L2_application_status,
la2.loan_status as L2_loan_status,
la2.lead_cost,
la2.campaign_name,
la2.isoriginated,
la2.origination_time as L2_Origination_time,
ifnull(lbl2.paid_off_date, la2.last_paymentdate) as L2_paidoff_date,
datediff(la2.origination_time,ifnull(lbl1.paid_off_date,la1.last_paymentdate)) as Days_Funded_since_L1_paidoff,
if(la2.lead_cost>1 and la2.loan_sequence>1,1,0) as RC_External,
la1.campaign_name,
la1.loan_sequence,
la1.application_status as L1_application_status,
la1.loan_status as L1_loan_status,
ifnull(lbl1.paid_off_date,la1.last_paymentdate) as L1_paidoff_date1
from reporting.leads_accepted la2
join jaglms.lms_base_loans lbl2 on la2.loan_number=lbl2.base_loan_id
left join reporting.leads_accepted la1 on la2.loan_sequence=la1.loan_sequence+1 and la1.lms_customer_id=la2.lms_customer_id and la1.lms_code=la2.lms_code and la1.isoriginated=1 -- L1
left join jaglms.lms_base_loans lbl1 on la1.loan_number=lbl1.base_loan_id
where 
la2.lms_code = 'JAG' 
and la2.loan_sequence=2
and lbl2.loan_status in ('Paid Off Loan','Paid Off','Originated')
and la2.isoriginated=1
and la2.IsApplicationTest=0
and la2.origination_time between date_sub(curdate(),interval 31 day) and date_sub(curdate(),interval 1 day) 
group by la2.lms_customer_id, la2.lms_application_id;



select loan_sequence, lms_code, lms_customer_id, isexpress, lead_sequence_id, lms_application_id, loan_number, state, product, loan_status, 
application_status, received_time, isoriginated, last_paymentdate, effective_date, lead_cost, storename, origination_time, origination_loan_id,
provider_name from reporting.leads_accepted where leads_accepted.lms_customer_id=1014925;

select * from jaglms.lms_base_loans where customer_id=701424 ;


##List2 - who never re-apply sincr L1 
select
la.lms_code, 
la.lms_customer_id, 
la.lms_application_id,

la.loan_number,
la.loan_sequence, 
la.loan_status,
la.origination_loan_id,
la.lead_cost,
la.campaign_name,
la.isoriginated,
la.application_status,
la.loan_status,
la.last_paymentdate,
datediff(curdate(),lbl.paid_off_date) as days_since_paidoff,
lbl.paid_off_date,
la2.lms_application_id as L2_application_id
from reporting.leads_accepted la
join jaglms.lms_base_loans lbl on la.loan_number=lbl.base_loan_id
join jaglms.lms_payment_schedules lps on la.lms_customer_id=lps.customer_id and la.loan_number=lps.base_loan_id 
join jaglms.lms_payment_schedule_items lpsi on lps.payment_schedule_id = lpsi.payment_schedule_id and lpsi.status='Cleared' and lpsi.total_amount>0 
left join reporting.leads_accepted la2 on la.lms_customer_id=la2.lms_customer_id and la.lms_code=la2.lms_code and la2.loan_sequence=la.loan_sequence+1 
where 
la.lms_code = 'JAG'
and la.loan_sequence=1
and lbl.loan_status in ('Paid Off Loan','Paid Off')
and la.isoriginated=1
and la.IsApplicationTest=0
and (lbl.paid_off_date between date_sub(curdate(),interval 60 day) and date_sub(curdate(),interval 30 day))
and la2.lms_application_id is null
group by la.lms_customer_id, la.lms_application_id;

