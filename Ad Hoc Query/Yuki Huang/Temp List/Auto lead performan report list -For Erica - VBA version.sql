





##MK - Bad Contact/Possible Fraud Lead Details - Previous 14 days -NC/RC
select case 
            when n.provider_name in('Acquire Interactive','Acquire Interactive AT') then 'ACQUIRE MK'
            when n.provider_name in('Arrowshade','Arrowshade AT') then 'ARROWSHADE MK'
            when n.provider_name ='Bloom Financial' then 'BLOOM MK'
            when n.provider_name ='EPCVIP' then 'EPCVIP MK'
            when n.provider_name ='Fix Media' then 'FIX MEDIA MK'
            when n.provider_name ='Green LLC' then 'GREEN LLC MK'
            when n.provider_name ='Gulf Coast Leads' then 'GCL MK'
      when n.provider_name ='Intimate Interactive' then 'II MK'
      when n.provider_name ='ITMEDIA' then 'ITMEDIA MK'
      when n.provider_name ='Lead Economy' then 'LEAD ECONOMY MK'      
      when n.provider_name ='Lead Flash' then 'LEAD FLASH MK'
      when n.provider_name ='Lead Toro' then 'LEAD TORO MK'
      when n.provider_name ='Lead Zoom' then 'LEAD ZOOM MK'
      when n.provider_name in('Leads Market','Leads Market AT','LOAN CALL') then 'LEADS MARKET MK'
      when n.provider_name ='Leap Theory' then 'LEAP THEORY MK'
      when n.provider_name in('Ping Logix','Ping Logix AT') then 'PINGLOGIX MK'   -- 'Ping Logix' then 'PINGLOGIX MK'
      when n.provider_name ='PING YO' then 'PINGYO MK'
      when n.provider_name ='Point Advertising' then 'POINT MK'
      when n.provider_name ='RoundSky' then 'ROUNDSKY MK'
      when n.provider_name ='Stop Go Network' then 'SGN MK'
      when n.provider_name in('Store Front Lender','Store Front Lender AT') then 'STORE FRONT LENDER MK'
      when n.provider_name in('Zero Parallel','Zero Parallel AT') then 'ZERO PARALLEL MK'
      ##when n.provider_name ='Avenue Link' then 'AVENUE LINK MK' -- ??
      else NULL
      end as Group_provider_name,n.state,n.campaign_name,n.lead_cost,
      n.original_lead_received_date, n.is_originated as Loan,n.application_status,
      n.withdrawn_reason, n.aff_id, n.sub_id, n.email_address
      
      from reporting.AFR_Normal n
      left join reporting.AFR_Normal_extra ne on n.lms_code = ne.lms_code and n.original_lead_id = ne.original_lead_id
      where n.withdrawn_reason in('Bad Contact Information','Possible Fraud') and withdrawn_datetime>= date_sub(curdate(), interval 14 day)
                  and n.withdrawn_datetime<curdate()
                  and n.lead_cost>1
            
union



##CF - Bad Contact/Possible Fraud Lead Details - Previous 14 days -NC/RC


select n.provider_name as Group_provider_name,n.state,n.campaign_name,n.lead_cost,
n.original_lead_received_date, n.is_originated as Loan,n.application_status,
n.withdrawn_reason, n.aff_id, n.sub_id, n.email_address

from reporting_cf.AFR_Normal n
where withdrawn_reason in('Bad Contact Information','Possible Fraud') and withdrawn_datetime>= date_sub(curdate(), interval 14 day)
            and withdrawn_datetime<curdate()
            and lead_cost>1; -- and lead_sequence_id>0;

            
            
            
            
            
            
            
            
            
            
            
           
            
            
            
            
            
             





####MK - Lead_performace - NC only - Previous 60 days

select 
case 
      when provider_name in('Acquire Interactive','Acquire Interactive AT') then 'ACQUIRE MK'
      when provider_name in('Arrowshade','Arrowshade AT') then 'ARROWSHADE MK'
      when provider_name ='Bloom Financial' then 'BLOOM MK'
      when provider_name ='EPCVIP' then 'EPCVIP MK'
      when provider_name ='Fix Media' then 'FIX MEDIA MK'
      when provider_name ='Green LLC' then 'GREEN LLC MK'
      when provider_name ='Gulf Coast Leads' then 'GCL MK'
when provider_name ='Intimate Interactive' then 'II MK'
when provider_name ='ITMEDIA' then 'ITMEDIA MK'
when provider_name ='Lead Economy' then 'LEAD ECONOMY MK'      
when provider_name ='Lead Flash' then 'LEAD FLASH MK'
when provider_name ='Lead Toro' then 'LEAD TORO MK'
when provider_name ='Lead Zoom' then 'LEAD ZOOM MK'
when provider_name in('Leads Market','Leads Market AT','LOAN CALL') then 'LEADS MARKET MK'
when provider_name ='Leap Theory' then 'LEAP THEORY MK'
when provider_name in('Ping Logix','Ping Logix AT') then 'PINGLOGIX MK'   -- 'Ping Logix' then 'PINGLOGIX MK'
when provider_name ='PING YO' then 'PINGYO MK'
when provider_name ='Point Advertising' then 'POINT MK'
when provider_name ='RoundSky' then 'ROUNDSKY MK'
when provider_name ='Stop Go Network' then 'SGN MK'
when provider_name in('Store Front Lender','Store Front Lender AT') then 'STORE FRONT LENDER MK'
when provider_name in('Zero Parallel','Zero Parallel AT') then 'ZERO PARALLEL MK'
-- when provider_name ='Avenue Link' then 'AVENUE LINK MK' -- ??
else NULL
end as Group_provider_name,
n.state,n.campaign_name,n.lead_cost,original_lead_received_date,n.is_originated as Loan,
n.application_status,withdrawn_reason, n.aff_id,n.sub_id, n.email_address,
if(sum(if(t.page in('welcome', 'welcome-RAL') and (t.`timestamp` between date_sub(ne.lead_received_time, interval 2 minute) and date_add(ne.lead_received_time, interval 5 minute)),1,0))>0,1,0) as Is_redirect
    
from reporting.AFR_Normal n
left join reporting.AFR_Normal_extra ne on n.lms_code=ne.lms_code and n.original_lead_id = ne.original_lead_id
left join webapi.tracking t on n.lead_sequence_id = t.lead_sequence_id and t.organization_id =1
where n.lms_code='JAG' and n.loan_sequence=1 and n.lead_cost>1
      and n.original_lead_received_date between date_sub(curdate(), interval 60 day) and date_sub(curdate(), interval 1 day)
group by n.lms_code, n.original_lead_id


union








####CF - Lead_performace - NC only - Previous 60 days
select 
n.provider_name as Group_provider_name,
n.state,n.campaign_name,n.lead_cost,original_lead_received_date,n.is_originated as Loan, 
n.application_status,withdrawn_reason, n.aff_id,n.sub_id, n.email_address,
-- n.lead_received_time,n.provider_name,n.original_lead_id,
if(sum(if(t.page in('welcome', 'welcome-RAL') and (t.`timestamp` between date_sub(n.lead_received_time, interval 2 minute) and date_add(n.lead_received_time, interval 5 minute)),1,0))>0,1,0) as Is_redirect
  
from reporting_cf.AFR_Normal n
left join webapi.tracking t on n.lead_sequence_id = t.lead_sequence_id and t.organization_id =2
where n.lms_code='JAG' and n.loan_sequence=1 and n.lead_cost>1
      and n.original_lead_received_date between date_sub(curdate(), interval 60 day) and date_sub(curdate(), interval 1 day) and n.channel='External'
group by n.lms_code, n.original_lead_id;




