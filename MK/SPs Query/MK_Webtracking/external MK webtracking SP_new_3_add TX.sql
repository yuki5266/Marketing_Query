DROP PROCEDURE IF EXISTS temp.MK_Web_Tracking_External;
CREATE PROCEDURE temp.`MK_Web_Tracking_External`(in std_date date, end_date date)
BEGIN
set @std_date= std_date, 
@end_date= end_date;

select    
n.*, la.received_time,        
--           ##non_ral
--           if(sum(if(t.page='welcome' and (t.`timestamp` between date_sub(la.received_time, interval 2 minute) and date_add(la.received_time, interval 5 minute)),1,0))>0,1,0) as Is_redirect,
--           if(sum(if(t.page='welcome' and la.received_time<=t.`timestamp`,1,0))>0,1,0) as 1_visit_wel,
-- 					if(sum(if(t.page='welcome_submit' and la.received_time<=t.`timestamp`,1,0))>0,1,0) as 1_submit_wel,
--           
          ##RAL
          if(sum(if(t.page='welcome-RAL' and (t.`timestamp` between date_sub(la.received_time, interval 2 minute) and date_add(la.received_time, interval 5 minute)),1,0))>0,1,0) as Is_redirect_ral,
          if(sum(if(t.page='welcome-RAL' and la.received_time<=t.`timestamp`,1,0))>0,1,0) as 1_visit_wel_ral,
					if(sum(if(t.page='welcome_submit-RAL' and la.received_time<=t.`timestamp`,1,0))>0,1,0) as 1_submit_wel_ral,
          
          #ALL
          if(sum(if(t.page in('welcome', 'welcome-RAL') and (t.`timestamp` between date_sub(la.received_time, interval 2 minute) and date_add(la.received_time, interval 5 minute)),1,0))>0,1,0) as Is_redirect_all,
          if(sum(if(t.page in('welcome', 'welcome-RAL') and la.received_time<=t.`timestamp`,1,0))>0,1,0) as 1_visit_wel_all,
          if(sum(if(t.page in('welcome', 'welcome-RAL') and date_sub(la.received_time, interval 2 minute)<=t.`timestamp`,1,0))>0,1,0) as 1_visit_wel_all_test,
          
					if(sum(if(t.page in('welcome_submit', 'welcome_submit-RAL') and la.received_time<=t.`timestamp`,1,0))>0,1,0) as 1_submit_wel_all,
          
					if(sum(if(t.page='create_account' and la.received_time<=t.`timestamp`,1,0))>0,1,0) as 2_create_account,
					if(sum(if(t.page='create_account_submit' and la.received_time<=t.`timestamp`,1,0))>0,1,0) as 2_submit_create_account,
					if(sum(if(t.page='account_terms' and la.received_time<=t.`timestamp`,1,0))>0,1,0) as 3_visit_consent,
          if(sum(if(t.page='account_terms_submit' and la.received_time<=t.`timestamp`,1,0))>0,1,0) as 3_submit_consent,
          if(sum(if(t.page='phone_verification' and la.received_time<=t.`timestamp`,1,0))>0,1,0) as 4_phone_verification,
          if(sum(if(t.page='phone_verification_submit' and la.received_time<=t.`timestamp`,1,0))>0,1,0) as 4_submit_phone_verification,
          ##TN Loan doc
          if(sum(if(la.state='TN' and t.page='loan_docs-CI' and la.received_time<=t.`timestamp`,1,0))>0,1,0) as reached_loan_doc_TN,
          if(sum(if(la.state='TN' and t.page='loan_docs-CI' and la.received_time<=t.`timestamp`,1,0))>0,1,0) as 5a_loan_docs_CI_TN,
          if(sum(if(la.state='TN' and t.page='loan_docs_submit-CI' and la.received_time<=t.`timestamp`,1,0))>0,1,0) as 5a_submit_loan_docs_CI_TN,
          if(sum(if(la.state='TN' and t.page='loan_docs-TC' and la.received_time<=t.`timestamp`,1,0))>0,1,0) as 5b_loan_docs_TC_TN,
          if(sum(if(la.state='TN' and t.page='loan_docs_submit-TC' and la.received_time<=t.`timestamp`,1,0))>0,1,0) as 5b_submit_loan_docs_TC_TN,
          if(sum(if(la.state='TN' and t.page='loan_docs-TILA' and la.received_time<=t.`timestamp`,1,0))>0,1,0) as 5c_loan_docs_TILA_TN,
          if(sum(if(la.state='TN' and t.page='loan_docs_submit-TILA' and la.received_time<=t.`timestamp`,1,0))>0,1,0) as 5c_submit_loan_docs_TILA_TN, 
          if(sum(if(la.state='TN' and t.page='loan_docs-ACH' and la.received_time<=t.`timestamp`,1,0))>0,1,0) as 5d_loan_docs_ACH_TN,
          if(sum(if(la.state='TN' and t.page='loan_docs_submit-ACH' and la.received_time<=t.`timestamp`,1,0))>0,1,0) as 5d_submit_loan_docs_ACH_TN,
          if(sum(if(la.state='TN' and t.page='loan_docs-ARB' and la.received_time<=t.`timestamp`,1,0))>0,1,0) as 5e_loan_docs_ARB_TN,
          if(sum(if(la.state='TN' and t.page='loan_docs_submit-ARB' and la.received_time<=t.`timestamp`,1,0))>0,1,0) as 5e_submit_loan_docs_ARB_TN,
          if(sum(if(la.state='TN' and t.page='loan_docs_submit-ARB' and la.received_time<=t.`timestamp`,1,0))>0,1,0) as completed_loan_doc_TN,
          
          ##TX Loan doc
          if(sum(if(la.state='TX' and t.page='loan_docs-CI' and la.received_time<=t.`timestamp`,1,0))>0,1,0) as reached_loan_doc_TX,
          if(sum(if(la.state='TX' and t.page='loan_docs-CI' and la.received_time<=t.`timestamp`,1,0))>0,1,0) as 5a_loan_docs_CI_TX,
          if(sum(if(la.state='TX' and t.page='loan_docs_submit-CI' and la.received_time<=t.`timestamp`,1,0))>0,1,0) as 5a_submit_loan_docs_CI_TX,
          if(sum(if(la.state='TX' and t.page='loan_docs-CSD' and la.received_time<=t.`timestamp`,1,0))>0,1,0) as 5b_loan_docs_CSD_TX,
          if(sum(if(la.state='TX' and t.page='loan_docs_submit-CSD' and la.received_time<=t.`timestamp`,1,0))>0,1,0) as 5b_submit_loan_docs_CSD_TX,
          if(sum(if(la.state='TX' and t.page='loan_docs-CSA' and la.received_time<=t.`timestamp`,1,0))>0,1,0) as 5c_loan_docs_CSA_TX,
          if(sum(if(la.state='TX' and t.page='loan_docs_submit-CSA' and la.received_time<=t.`timestamp`,1,0))>0,1,0) as 5c_submit_loan_docs_CSA_TX, 
          if(sum(if(la.state='TX' and t.page='loan_docs-PP' and la.received_time<=t.`timestamp`,1,0))>0,1,0) as 5d_loan_docs_PP_TX,
          if(sum(if(la.state='TX' and t.page='loan_docs_submit-PP' and la.received_time<=t.`timestamp`,1,0))>0,1,0) as 5d_submit_loan_docs_PP_TX,
          if(sum(if(la.state='TX' and t.page='loan_docs-LA' and la.received_time<=t.`timestamp`,1,0))>0,1,0) as 5e_loan_docs_LA_TX,
          if(sum(if(la.state='TX' and t.page='loan_docs_submit-LA' and la.received_time<=t.`timestamp`,1,0))>0,1,0) as 5e_submit_loan_docs_LA_TX,
          if(sum(if(la.state='TX' and t.page='loan_docs-ACH' and la.received_time<=t.`timestamp`,1,0))>0,1,0) as 5f_loan_docs_ACH_TX,
          if(sum(if(la.state='TX' and t.page='loan_docs_submit-ACH' and la.received_time<=t.`timestamp`,1,0))>0,1,0) as 5f_submit_loan_docs_ACH_TX,
          if(sum(if(la.state='TX' and t.page='loan_docs_submit-ACH' and la.received_time<=t.`timestamp`,1,0))>0,1,0) as completed_loan_doc_TX,
          
          
          if(sum(if(t.page='user_account' and la.received_time<=t.`timestamp`,1,0))>0,1,0) as 6_view_dashboard_account,
          
          if(sum(if(t.page='user_bankverification' and la.received_time<=t.`timestamp`,1,0))>0,1,0) as 7_confirm_BV,
          if(sum(if(t.page like '%-ro%' and la.received_time<=t.`timestamp`,1,0))>0,1,0) as is_rollback
                              
from reporting.AFR_Normal n
inner join reporting.leads_accepted la on n.customer_id=la.lms_customer_id  and n.original_lead_id=la.lms_application_id
left join webapi.tracking t on la.lms_customer_id = t.lms_customer_id and t.organization_id =1
where la.provider_name!='Money Key Web' and la.provider_name not like '%test%' and la.IsApplicationTest=0
      and n.lms_code='JAG'
      and n.original_lead_received_date between @std_date and @end_date
      and (case when n.product='LOC' and n.state='TN' and la.received_time>='2019-09-25' then 1
            when n.state='TX' and la.received_time>='2019-11-15' then 1
            else 0
       end)=1
group by la.lms_code, la.lms_customer_id, la.lms_application_id;

END;
