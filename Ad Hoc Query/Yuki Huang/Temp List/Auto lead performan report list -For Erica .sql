
/*
I would like to create some automated reports that we can send to the lead providers. There are 2 reports I would like to set-up, for both MoneyKey and CreditFresh:
 
1)	Bad Contact / Possible Fraud Lead Details – for previous 14 days
2)	Lead Performance – for previous 60 days
 
Attached please find examples of the both – which include the fields I need to have included. Both reports can run bi-weekly on a Monday,
for the previous 14 days or previous 60 days. 

Notes:
-	NC only for Lead Performance
-	NC + RC for Bad Contact / Possible Fraud
-	Can the files be password protected/secured? I will be sending them to external lead providers. 
-	I will need them for both CreditFresh and MoneyKey. So for example, Acquire Interactive would have 4 files. 
-	The providers will need to be grouped – i.e. Acquire Interactive + Acquire Interactive AT. The groupings are attached. 
-	Please name the files in this format (or similar):
o	ACQUIRE MK Bad Contact Leads Oct 13-27 2019
o	ACQUIRE MK Lead Performance Oct 13-27 2019
o	ACQUIRE CF Bad Contact Leads Oct 13-27 2019 
o	ACQUIRE CF Lead Performance Oct 13-27 2019

*/



####MK

-- select * from reporting.withdrawn_reason_code;

-- sendgrid_statistics ;


-- select * from jaglms.uw_secondary_logging limit 100;

-- select distinct noaa_message  from jaglms.uw_secondary_logging;


##external NC/RC from HB;break down by lead provider

select lms_code, nc_lms_accepts, loan_sequence, lms_display_number,original_lead_received_date,  customer_id, state,lead_sequence_id,email_address, withdrawn_reason, withdrawn_reason_detail,withdrawn_datetime,  lead_cost,
      provider_name, campaign_name,  aff_id, sub_id,
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
when provider_name ='Avenue Link' then 'AVENUE LINK MK' -- ??
else NULL
end as Group_provider_name   
from reporting.AFR_Normal 
where withdrawn_reason in('Bad Contact Information','Possible Fraud') and withdrawn_datetime>= date_sub(curdate(), interval 14 day)
            and withdrawn_datetime<curdate()
            and lead_sequence_id>0 and lead_cost>1;



####
select n.lms_code, n.nc_lms_accepts, loan_sequence, lms_display_number, customer_id, withdrawn_reason, withdrawn_reason_detail, withdrawn_datetime, 
n.lead_sequence_id, lead_cost,
      provider_name, campaign_name, original_lead_received_date, aff_id, sub_id,     
      if(sum(if(t.page='welcome' and (t.`timestamp` between date_sub(ne.lead_received_time, interval 2 minute) and date_add(ne.lead_received_time, interval 5 minute)),1,0))>0,1,0) as Is_redirect,
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
when provider_name ='Avenue Link' then 'AVENUE LINK MK' -- ??
else NULL
end as Group_provider_name   
from reporting.AFR_Normal n
left join reporting.AFR_Normal_extra ne on n.original_lead_id = ne.original_lead_id
left join webapi.tracking t on n.lead_sequence_id = t.lead_sequence_id and t.organization_id =1
where n.lms_code='JAG' and n.loan_sequence=1 and n.lead_cost>1
      and n.original_lead_received_date between date_sub(curdate(), interval 60 day) and date_sub(curdate(), interval 1 day)
group by n.lms_code, n.original_lead_id;