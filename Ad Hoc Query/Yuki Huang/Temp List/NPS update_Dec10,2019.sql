-- to exclude customers from OH

-- Add a new group (list_name) "LOC DD" and include any customer who drew from their LOC the previous day and has a draw sequence equal to or greater than 3

select
'NPS' as List_Name,
CONCAT(UCASE(SUBSTRING(la.customer_firstname, 1, 1)),LOWER(SUBSTRING(la.customer_firstname, 2))) as Customer_FirstName,
CONCAT(UCASE(SUBSTRING(la.customer_lastname, 1, 1)),LOWER(SUBSTRING(la.customer_lastname, 2))) as Customer_LastName,
la.age,
la.lms_customer_id,
la.emailaddress, 
la.lms_code,
la.product,
la.state,
ifnull(la.campaign_name,la2.campaign_name) as Campaign_name,
la.origination_time,
la.loan_sequence,
la.isreturning,
ifnull(la2.is_ral,0) as Is_ral,
if(la2.lead_cost>1, 1,0) as is_external,
la.agent as loan_assigned_agent,
la.approved_amount,
la.storename as entity
from reporting.leads_accepted la
left join reporting.leads_accepted la2 on la.original_lead_id=la2.original_lead_id 
                         and la2.lms_code='JAG' and la2.origination_loan_id is not null
where la.lms_code = 'JAG' and la.effective_date = curdate() and la.loan_status!='Voided' and la.state!='OH'


union 

################JAG PO - All state, JAG Only

				
			select distinct
      'JAG PO' as List_Name,
      CONCAT(UCASE(SUBSTRING(la.customer_firstname, 1, 1)),LOWER(SUBSTRING(la.customer_firstname, 2))) as Customer_FirstName,
			CONCAT(UCASE(SUBSTRING(la.customer_lastname, 1, 1)),LOWER(SUBSTRING(la.customer_lastname, 2))) as Customer_LastName,
      la.age,
      la.lms_customer_id,
      la.emailaddress,
      la.lms_code,
			la.product,
      la.state,
      ifnull(la.campaign_name,la2.campaign_name) as Campaign_name,
      la.origination_time,
      la.loan_sequence,
       la.isreturning,
      ifnull(la2.is_ral,0) as Is_ral,
       if(la.lead_cost >1,1,0) as Is_External,
        la.agent as loan_assigned_agent,
       la.approved_amount,
la.storename as entity
      from reporting.leads_accepted la
      left join reporting.leads_accepted la2 on la.original_lead_id = la2.original_lead_id and la2.origination_loan_id is not null and la2.lms_code = 'JAG' 
			inner join jaglms.lms_base_loans b on la.lms_application_id =b.loan_header_id
			inner join jaglms.lms_client_transactions tr on b.base_loan_id=tr.base_loan_id
      where la.loan_status = 'Pending Paid Off'
			and  tr.trans_type in ('Debit', 'D')
			and date(tr.trans_date)=curdate()
			and date(tr.trans_date) >= date(la.last_paymentdate)
			and SUBSTR(SUBSTR(la.emailaddress, INSTR(la.emailaddress, '@'), INSTR(la.emailaddress, '.')), 2) not in ('epic.lmsmail.com', 'moneykey.com')
			and cast(la.loan_number as unsigned) not in (select distinct base_loan_id from jaglms.collection_lms_loan_map)
      and la.state!='OH'
		union 
      
      
      
      ##########LOC DD -->include any customer who drew from their LOC the previous day and has a draw sequence equal to or greater than 3
                       -- 
                       
select 

aa.List_Name,
aa.Customer_FirstName,
aa.Customer_LastName,
      aa.age,
      aa.lms_customer_id,
      aa.emailaddress,
      aa.lms_code,
      aa.product, 
      aa.state, 
     aa.campaign_name,
      aa.origination_time,
      aa.loan_sequence,
       aa.isreturning,
       aa.is_ral,
       aa.IsExternal,
       aa.loan_assigned_agent,
       aa.approved_amount,
aa.entity
from 
      (select 'LOC DD' as List_Name,
     CONCAT(UCASE(SUBSTRING(la.customer_firstname, 1, 1)),LOWER(SUBSTRING(la.customer_firstname, 2))) as Customer_FirstName,
			CONCAT(UCASE(SUBSTRING(la.customer_lastname, 1, 1)),LOWER(SUBSTRING(la.customer_lastname, 2))) as Customer_LastName,
      la.age,
      la.lms_customer_id,
      la.emailaddress,
      la.lms_code,
      la.product, 
      la.state, 
     la.campaign_name,
      la.origination_time,
      la.loan_sequence,
       la.is_ral,
       la.IsExternal,
        la.agent as loan_assigned_agent,
la.storename as entity,
       la.lms_application_id, 
       la.loan_number as loan_id,la.original_lead_id, 
      la.pay_frequency,
      la.isreturning, 
      la.received_time, 
      la.application_status,
      la.loan_status, 
      la.last_paymentdate,
      la.approved_amount, 
      ps.is_active,
      max(psi.item_date) as last_draw_date,
      psi.item_type,
      psi.status,
      count(psi.item_date) as draw_sequence
      from reporting.leads_accepted la
      join jaglms.lms_payment_schedules ps on la.loan_number = ps.base_loan_id and ps.is_collections =0
      join jaglms.lms_payment_schedule_items psi on ps.payment_schedule_id = psi.payment_schedule_id and psi.total_amount<0 and psi.status in ('Cleared', 'SENT') 
            and psi.item_date <=curdate() 
      where la.isoriginated = 1 and la.state!='OH'
      and la.loan_status ='originated'
      group by la.lms_customer_id) aa
where aa.last_draw_date = date_sub(curdate(),interval 1 day)
and draw_sequence >=3;