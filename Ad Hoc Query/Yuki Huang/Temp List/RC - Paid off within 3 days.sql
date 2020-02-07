set @std_date= '2018-01-01',
@end_date= '2019-08-31',
@track_end_date='2019-09-22';



DROP TEMPORARY TABLE IF EXISTS ecust;
CREATE TEMPORARY TABLE IF NOT EXISTS ecust ( INDEX(lms_customer_id, lms_application_id) ) 
AS (

select la.lms_code, la.lms_customer_id, la.lms_application_id,la.state, la.loan_number,
      la.product,la.received_time, la.pay_frequency, 
      la.approved_amount, ifnull(la.campaign_name, '') campaign_name, 
      la.loan_sequence, la.origination_time, 
      max(vp.EffectiveDate) as last_payment_date,
      max(vl.RenewalExtensions+1) as last_rew_num
      ,sum(vp.feesamount) as fee_income  
      ,if(la.loan_status='Pending Paid Off',1,0) as Is_Pending_Paid_Off
      ,la.loan_status
from reporting.leads_accepted la
join ais.vw_loans vl on la.lms_customer_id=vl.DebtorClientId and la.lms_application_id=if(vl.OriginalLoanId=0,vl.Id,vl.OriginalLoanId)
join ais.vw_payments vp on vl.id=vp.LoanId and vp.IsDebit=1 and vp.paymentstatus='Checked'
where
la.isoriginated=1 
and la.loan_status in ('Charged Off Paid Off','Paid Off Loan','Returned Item Paid Off', 'Settlement Paid Off', 'Pending Paid Off')
and la.lms_code='EPIC'

and la.IsApplicationTest=0
group by la.lms_application_id
having  max(vp.EffectiveDate) between @std_date and @end_date
)
;




DROP TEMPORARY TABLE IF EXISTS jcust;
CREATE TEMPORARY TABLE IF NOT EXISTS jcust ( INDEX(lms_customer_id, lms_application_id) ) 
AS (

select la.lms_code, la.lms_customer_id, la.lms_application_id,la.state, la.loan_number, 
      la.product,la.received_time, la.pay_frequency, 
      la.approved_amount, ifnull(la.campaign_name, '') campaign_name, la.loan_sequence, la.origination_time, 
      max(psi.item_date) as last_payment_date,
      sum(if(psi.item_type !='C' and ps.is_collections=0,1,0)) as last_rew_num
      ,sum(psi.amount_fee) as fee_income 
      ,if(lbl.loan_status='Pending Paid Off',1,0) as Is_Pending_Paid_Off
      ,lbl.loan_status
from reporting.leads_accepted la
join jaglms.lms_base_loans lbl on cast(la.loan_number as unsigned) =lbl.base_loan_id and lbl.loan_status in ('DEFAULT-PIF', 'DEFAULT-SIF', 'Paid Off', 'Pending Paid Off')
join jaglms.lms_payment_schedules ps on la.lms_customer_id=ps.customer_id  and  cast(la.loan_number as unsigned) =ps.base_loan_id 
join jaglms.lms_payment_schedule_items psi on ps.payment_schedule_id = psi.payment_schedule_id and psi.total_amount>0
where 

la.isoriginated=1 
and la.lms_code='JAG'
and psi.status ='Cleared'
and la.IsApplicationTest=0
group by la.lms_application_id
having max(psi.item_date) between @std_date and @end_date

)
;


DROP TEMPORARY TABLE IF EXISTS base;
CREATE TEMPORARY TABLE IF NOT EXISTS base ( INDEX(lms_customer_id, lms_application_id) ) 
AS (

select * from
(
select * from ecust
union
select * from jcust
) bt

);





DROP TEMPORARY TABLE IF EXISTS track_app;
CREATE TEMPORARY TABLE IF NOT EXISTS track_app
AS (

select b.lms_code, 
       b.lms_customer_id, 
       if(b.lms_code='EPIC', b.lms_application_id, b.loan_number) as paid_off_loan_id,
        b.state, 
       b.product,b.received_time, b.loan_sequence,
b.approved_amount,
b.campaign_name,
b.pay_frequency, b.last_rew_num,
b.origination_time , 
date(b.last_payment_date) as last_payment_date,
b.loan_status,
if(la.lms_application_id is not null,1,0) add_application,
la.lms_application_id as lms_application_id2,  
la.loan_sequence loan_sequence2,
la.received_time received_time2, 
ifnull(datediff(la.received_time, b.last_payment_date),'') as day_to_add_application,
if(la.origination_loan_id is not null,1,0) add_origination,
la.origination_loan_id as add_loan_id, 
la2.origination_time origination_time2,
ifnull(datediff(la2.origination_time,b.last_payment_date),'') as day_to_add_origination, 
ifnull(la2.approved_amount,'') as approved_amount2,
b.Is_Pending_Paid_Off,
case when la.lead_cost>1 then 'External'
     when la.lms_customer_id is null then null
     else 'Internal'
end as RC_Channel
from base b
left join reporting.leads_accepted la on b.lms_code=la.lms_code and b.lms_customer_id=la.lms_customer_id and (b.loan_sequence+1)=la.loan_sequence
          and date(la.received_time) between b.last_payment_date and @track_end_date
          and la.isuniqueaccept=1
   
left join reporting.leads_accepted la2 on la.lms_code=la2.lms_code and la.lms_customer_id=la2.lms_customer_id and la.origination_loan_id=if(la.lms_code='JAG', cast(la2.loan_number as unsigned),la2.lms_application_id)  


);

DROP TEMPORARY TABLE IF EXISTS unique_track_app;
CREATE TEMPORARY TABLE IF NOT EXISTS unique_track_app
AS (
select lms_code, lms_customer_id, paid_off_loan_id, state, product, received_time,  loan_sequence,
approved_amount,  campaign_name, pay_frequency,  last_rew_num,
origination_time , last_payment_date, 
 datediff(last_payment_date, origination_time) as days_paidoff_after_origination,
loan_status,
RC_Channel,
if(sum(add_application)>0,1,0) as add_application,
lms_application_id2,  loan_sequence2, received_time2, day_to_add_application,
if(sum(add_origination)>0,1,0) as add_origination,  max(add_loan_id) as add_loan_id, 
max(origination_time2) as origination_time2, max(day_to_add_origination) as day_to_add_origination, 
max(approved_amount2) as approved_amount2,
count(paid_off_loan_id) as total_new_apply_cnt  
 from track_app
group by lms_code, lms_customer_id, paid_off_loan_id
);


-- select * from track_app;
-- select * from unique_track_app where lms_code = 'JAG' and days_paidoff_after_origination <=3;


select * from jaglms.lms_payment_schedule_items where payment_schedule_id in
(select payment_schedule_id from jaglms.lms_payment_schedules where base_loan_id =426400);





select lms_customer_id,loan_number,effective_date from reporting.leads_accepted where loan_number in (426400
,465916
,969001
,1042502
,813833
,969089
,1052131
,849217)


-- 969089/1052131



DROP TEMPORARY TABLE IF EXISTS checker1;
CREATE TEMPORARY TABLE IF NOT EXISTS checker1
AS (
select uta.add_loan_id,ps.base_loan_id,psi.payment_schedule_id 
from unique_track_app uta
join jaglms.lms_payment_schedules ps on uta.paid_off_loan_id = ps.base_loan_id and uta.lms_customer_id = ps.customer_id
join jaglms.lms_payment_schedule_items psi on ps.base_loan_id = psi.payment_schedule_id);



select count(*) from unique_track_app;
select count(*) from checker1;
select * from checker1;







