/*EPIC*/


DROP TEMPORARY TABLE IF EXISTS table1;
CREATE TEMPORARY TABLE IF NOT EXISTS table1  
AS (
select *, count(*) as cnt
from 
      (select distinct
      la.lms_customer_id, 
      la.lms_application_id,
      la.loan_number,
      la.received_time,
      la.lms_code, 
      la.state, 
      la.product, 
      la.loan_sequence, 
      la.emailaddress as email,
      CONCAT(UCASE(SUBSTRING(la.customer_firstname, 1, 1)),LOWER(SUBSTRING(la.customer_firstname, 2))) as Customer_FirstName,
      CONCAT(UCASE(SUBSTRING(la.customer_lastname, 1, 1)),LOWER(SUBSTRING(la.customer_lastname, 2))) as Customer_LastName,
      max(vp.EffectiveDate) as last_payment_date,
      la.pay_frequency,
      sum(if(vp.IsDebit=1 and vp.PaymentStatus='Checked' and vp.PaymentType !='Cash', vp.principle*-1, if(vp.IsDebit=0 and vp.PaymentStatus='Checked' and vp.IsOrigination=1, vp.principle, null))) as principal_balance,
      sum(if(vp.IsDebit=1 and vp.PaymentStatus='Checked' and vp.PaymentType ='Cash', vp.principle, 0)) as write_off_Principal,
      sum(if(vp.IsDebit=1 and vp.PaymentStatus='Checked' and vp.PaymentType ='Cash', vp.PaymentAmount, null)) as write_off_amount,
      max(vp.successdate) as last_clear_date,
      sum(if(vp.IsDebit=1 and vp.PaymentStatus ='Rejected', 1, 0)) as Default_Cnt,
      vl.LoanStatus as Status,
      la.loan_status,
      if(vl.LoanStatus=la.loan_status,1,0) as IsSame,
      if(sum(if(vl.CollectionStartDate is not null, 1, 0))>0,1,0) as IsCollection
      from reporting.leads_accepted la 
      inner join ais.vw_loans vl on la.lms_application_id=if(vl.OriginalLoanId=0, vl.Id, vl.OriginalLoanId) AND vl.LoanStatus not in('DELETED','Voided Renewed Loan')
      left join ais.vw_payments vp on vl.Id=vp.LoanId and vp.PaymentStatus in ('Checked', 'Rejected')
      where la.isoriginated=1 and la.lms_code='EPIC' 
      and la.state ='TX'-- in ('DE','UT', 'SD')
      and la.IsApplicationTest=0
      group by la.lms_code, la.lms_customer_id, la.lms_application_id) filter
where principal_balance >0 -- !=0
and IsCollection=0
and principal_balance-write_off_Principal!=0
group by filter.lms_customer_id);



DROP TEMPORARY TABLE IF EXISTS table2;
CREATE TEMPORARY TABLE IF NOT EXISTS table2  
AS (select
la.lms_code,
la.lms_customer_id,
la.state,
la.product,
la.pay_frequency,
max(la.loan_sequence) as last_loan_sequence,
la.loan_status,
max(la.origination_time) as last_origination_time,
la.emailaddress
from reporting.leads_accepted la
where la.lms_code='EPIC'
and la.state='TX'
and la.origination_time>0
and la.loan_status in( 'New Loan','Renewed Loan''Pending Renewal Loan')
group by la.lms_customer_id,la.lms_code);


select * from table1;
select * from table2;
-- select * from table1 t1 where t1.lms_customer_id in ( select distinct lms_customer_id from table2);

select * from table2 t1 where t1.lms_customer_id not in ( select distinct lms_customer_id from table1);

select * from reporting.leads_accepted where lms_code='EPIC' and lms_customer_id=74484;


select * from ais.vw_payments  where loanid in (select id from ais.vw_loans where id = 4288689 or OriginalLoanId =4288689);
select * from ais.vw_loans where id =4288689 or OriginalLoanId =4288689;