DROP PROCEDURE IF EXISTS temp.MK_Web_Tracking_Internal_ALL;
CREATE PROCEDURE temp.`MK_Web_Tracking_Internal_ALL`()
BEGIN

DROP TEMPORARY TABLE IF EXISTS web_users;
CREATE TEMPORARY TABLE IF NOT EXISTS web_users ( INDEX(lead_sequence_id) ) 
AS (
select  t.`timestamp`, t.user_name, t.lms_customer_id, 
					if(sum(if(t.page='create_password_submit',1,0))>0,1,0) as 1_submit_create_password,
					if(sum(if(t.page='account_terms',1,0))>0,1,0) as 2_visit_consent,
          if(sum(if(t.page='account_terms_submit',1,0))>0,1,0) as 2_submit_consent,
          if(sum(if(t.page='personal_info',1,0))>0,1,0) as 3_personal_info,
          if(sum(if(t.page='personal_info_submit',1,0))>0,1,0) as 3_submit_personal_info,
          if(sum(if(t.page='address_info',1,0))>0,1,0) as 4_address_info,
          if(sum(if(t.page='address_info_submit',1,0))>0,1,0) as 4_submit_address_info,
          if(sum(if(t.page='living_situation',1,0))>0,1,0) as 5_living_situation,
          if(sum(if(t.page='living_situation_submit',1,0))>0,1,0) as 5_submit_living_situation, 
          if(sum(if(t.page='payment_schedule',1,0))>0,1,0) as 6_payment_schedule,
          if(sum(if(t.page='payment_schedule_submit',1,0))>0,1,0) as 6_submit_payment_schedule,
          
          if(sum(if(t.page='reject',1,0))>0 and sum(if(t.page='pre_approved_submit',1,0))=0,1,0) as Application_Reject_1,
          
          if(sum(if(t.page='pre_approved',1,0))>0,1,0) as 7_pre_approved,
          if(sum(if(t.page='pre_approved_submit',1,0))>0,1,0) as 7_submit_pre_approved,
          if(sum(if(t.page='employment_info',1,0))>0,1,0) as 8_employment_info,
          if(sum(if(t.page='employment_info_submit',1,0))>0,1,0) as 8_submit_employment_info,
          if(sum(if(t.page='banking_info',1,0))>0,1,0) as 9_banking_info,
          if(sum(if(t.page='banking_info_submit',1,0))>0,1,0) as 9_submit_banking_info,
          
          if(sum(if(t.page='reject',1,0))>0 and sum(if(t.page='pre_approved_submit',1,0))>0 
                                     and sum(if(t.page='kyc_submit',1,0))=0,1,0) as HB2_Reject_old,
          
          if(sum(if(t.page='reject' and n.lead_received_time<=t.`timestamp`,1,0))>0 and sum(if(t.page='banking_info_submit',1,0))>0,1,0)  as HB2_Reject,
          
          if(sum(if(t.page='loan_docs-CI',1,0))>0,1,0) as 10_reached_loan_doc,
          if(sum(if(t.page='loan_docs-CI',1,0))>0,1,0) as 10a_loan_docs_CI,
          if(sum(if(t.page='loan_docs_submit-CI',1,0))>0,1,0) as 10a_submit_loan_docs_CI,
          if(sum(if(t.page='loan_docs-TC',1,0))>0,1,0) as 10b_loan_docs_TC,
          if(sum(if(t.page='loan_docs_submit-TC',1,0))>0,1,0) as 10b_submit_loan_docs_TC,
          if(sum(if(t.page='loan_docs-TILA',1,0))>0,1,0) as 10c_loan_docs_TILA,
          if(sum(if(t.page='loan_docs_submit-TILA',1,0))>0,1,0) as 10c_submit_loan_docs_TILA, 
          if(sum(if(t.page='loan_docs-ACH',1,0))>0,1,0) as 10d_loan_docs_ACH,
          if(sum(if(t.page='loan_docs_submit-ACH',1,0))>0,1,0) as 10d_submit_loan_docs_ACH,
          if(sum(if(t.page='loan_docs-ARB',1,0))>0,1,0) as 10e_loan_docs_ARB,
          if(sum(if(t.page='loan_docs_submit-ARB',1,0))>0,1,0) as 10e_submit_loan_docs_ARB,
          if(sum(if(t.page='loan_docs_submit-ARB',1,0))>0,1,0) as completed_loan_doc,
          if(sum(if(t.page='user_account',1,0))>0,1,0) as 11_view_dashboard_account,
          if(sum(if(t.page like '%-ro%',1,0))>0,1,0) as is_rollback,
          max(t.lead_sequence_id) as lead_sequence_id
                              
from  webapi.tracking t 
left join reporting.z_AFR_Normal n on n.lead_sequence_id = t.lead_sequence_id
where t.user_name in 
            (select distinct t2.user_name
            from webapi.tracking t2
            where t2.`timestamp`>='2019-09-25' and t2.organization_id=1 and  t2.page='create_password_submit'
                  and t2.user_name not like '%moneykey.com'
            group by t2.user_name)
     and t.`timestamp`>='2019-09-25'
group by t.user_name);

select t.*,
       if(n.lms_code is not null,1,0) as is_LMS,
        n.*
from web_users t
left join reporting.z_AFR_Normal n on n.lead_sequence_id = t.lead_sequence_id;

END;
