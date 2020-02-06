
-- "is_short_form"logic: where la.provider_name!='CreditFresh Internal' and la.provider_name not like '%test%' and la.`IsApplicationTest`=0 and la.`campaign_name`  like '%-SF'


##LOC NL - New Originations : effective_date = today
/*
select
'NPS NL' as List_Name,
CONCAT(UCASE(SUBSTRING(la.customer_firstname, 1, 1)),LOWER(SUBSTRING(la.customer_firstname, 2))) as Customer_FirstName,
CONCAT(UCASE(SUBSTRING(la.customer_lastname, 1, 1)),LOWER(SUBSTRING(la.customer_lastname, 2))) as Customer_LastName,
la.age,
la.lms_customer_id,
la.emailaddress, 
la.lms_code,
la.product,
la.state,
la.Campaign_name,
la.origination_time,
la.loan_sequence,
la.isreturning,
la.Is_ral,
la.IsExternal,
if(la.provider_name!='CreditFresh Internal' and la.provider_name not like '%test%' and la.`IsApplicationTest`=0 and la.`campaign_name`  like '%-SF',1,0) as is_short_form,
la.agent as loan_assigned_agent,
la.approved_amount,
la.storename as entity,
la.effective_date
from reporting_cf.leads_accepted la
where la.effective_date = curdate() and la.loan_status!='Voided';  */


select
'NPS NL' as List_Name,
CONCAT(UCASE(SUBSTRING(la.customer_firstname, 1, 1)),LOWER(SUBSTRING(la.customer_firstname, 2))) as Customer_FirstName,
CONCAT(UCASE(SUBSTRING(la.customer_lastname, 1, 1)),LOWER(SUBSTRING(la.customer_lastname, 2))) as Customer_LastName,
n.customer_age,
n.customer_id,
n.email_address,
n.lms_code, 
n.product,
n.state,
n.campaign_name,
n.origination_datetime,
n.loan_sequence,
n.is_returning,
n.is_ral,
n.rc_external as IsExternal,
if(n.provider_name !='CreditFresh Internal' and n.provider_name not like '%test%' and la.`IsApplicationTest`=0 and (n.campaign_name  like '%-SF' or n.campaign_name  like '%-SF#'),1,0) as is_short_form,
la.agent as loan_assigned_agent,
n.application_approved_amount,
n.store_name as entity
from reporting_cf.AFR_Normal n
left join reporting_cf.leads_accepted la on n.customer_id=la.lms_customer_id and la.loan_sequence=1 and la.origination_time>0
where n.effective_date = curdate() and n.current_loan_status !='Voided'

union 


##LOC DD - have a draw sequence equal or greater than 3: the effective date of the last draw is today

select 
'NPS DD' as List_Name,
CONCAT(UCASE(SUBSTRING(aa.customer_firstname, 1, 1)),LOWER(SUBSTRING(aa.customer_firstname, 2))) as Customer_FirstName,
CONCAT(UCASE(SUBSTRING(aa.customer_lastname, 1, 1)),LOWER(SUBSTRING(aa.customer_lastname, 2))) as Customer_LastName,
aa.age,
aa.lms_customer_id,
aa.emailaddress, 
aa.lms_code,
aa.product,
aa.state,
aa.Campaign_name,
aa.origination_time,
aa.loan_sequence,
aa.isreturning,
aa.Is_ral,
aa.IsExternal,
aa.is_short_form,
aa.agent as loan_assigned_agent,
aa.approved_amount,
aa.storename as entity
from 
      (select la.lms_code,
      la.lms_customer_id,  
      la.emailaddress,
       la.customer_firstname,
       la.customer_lastname,
       la.lms_application_id, 
       la.loan_number as loan_id,
       la.original_lead_id, 
       la.age,
       la.is_ral,
       la.IsExternal,
      if(la.provider_name!='CreditFresh Internal' and la.provider_name not like '%test%' and la.`IsApplicationTest`=0 and (la.`campaign_name`  like '%-SF' or la.`campaign_name`  like '%-SF#'),1,0) as is_short_form,
       la.agent,
       la.storename,
      la.product, 
      la.state, 
      la.pay_frequency, 
      la.loan_sequence, 
      la.isreturning, 
      la.received_time,
      la.application_status, 
      la.loan_status, 
      la.origination_time, 
      la.last_paymentdate,
      la.approved_amount,
      la.campaign_name,
      ps.is_active,
      max(psi.item_date) as last_draw_date,
      psi.item_type,
      psi.status,
      count(psi.item_date) as draw_sequence
      from reporting_cf.leads_accepted la
      join jaglms.lms_payment_schedules ps on la.loan_number = ps.base_loan_id and ps.is_collections =0
      join jaglms.lms_payment_schedule_items psi on ps.payment_schedule_id = psi.payment_schedule_id and psi.total_amount<0 and psi.status in ('Cleared', 'SENT') 
            and psi.item_date <=curdate() 
      where la.isoriginated = 1
      and la.loan_status ='originated'
      group by la.lms_customer_id) aa
where 
aa.last_draw_date=curdate() 
and draw_sequence >=3;


