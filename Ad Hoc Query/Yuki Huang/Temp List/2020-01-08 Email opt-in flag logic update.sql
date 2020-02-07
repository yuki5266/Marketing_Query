##JAG: jaglms.lms_customer_info_flat ci on ci.customer_id=la.lms_customer_id


SELECT 
customer_id, 
lastname, 
firstname,
address, 
city, 
state, 
email, 
ssn_last_four,
optout_account_email, 
optout_account_sms, 
optout_marketing_sms, 
optout_marketing_email, 
last_update
FROM jaglms.lms_customer_info_flat 
where last_update>='2020-01-01' limit 1000;


select ch.*,
lci.optout_account_email, 
lci.optout_marketing_email,
if(lci.optout_account_email='false' or lci.optout_account_email is null,1,0) as is_transactional_optin_email,
if(lci.optout_marketing_email='false' or lci.optout_marketing_email is null,1,0) as is_marketing_optin_email
from reporting.loc_gc_campaign_history ch 
inner join jaglms.lms_customer_info_flat lci on lci.customer_id=ch.lms_customer_id and ch.job_ID like '%JAGLOC%' 
where ch.list_generation_date>='2019-12-01'  LIMIT 100;




select * from reporting.loc_gc_campaign_history limit 100;
select distinct job_ID from reporting.loc_gc_campaign_history;






-- need to update campaign_history's column, differentiate 4 type of opt-in flag,email and SMS

Update reporting.campaign_history ch 
inner join jaglms.lms_customer_info_flat lci on lci.customer_id=ch.lms_customer_id and ch.lms_code='JAG'  
set ch.is_transactional_optin=if(lci.optout_account_email='false',1,0),
    ch.is_marketing_optin_email=if(optout_marketing_email='false',1,0)
where ch.business_date=curdate();


Update reporting.loc_gc_campaign_history ch 
inner join jaglms.lms_customer_info_flat lci on lci.customer_id=ch.lms_customer_id and ch.job_ID like '%JAGLOC%' 
set ch.is_transactional_optin=if(lci.optout_account_email='false',1,0),
    ch.is_marketing_optin_email=if(optout_marketing_email='false',1,0)
where ch.business_date=curdate();


##EPIC: ais.vw_client on cl.id=la.lms_customer_id


select 
Id,
FirstName,
MiddleName,  
LastName,
SSN, 
SSN4, 
EmailAddress, 
Email_MarketingOptIn, 
Email_OperationalOptIn,
Sms_OperationalOptIn, 
Sms_MarketingOptIn,
DateChanged, 
DateCreated  
from ais.vw_client
where DateChanged>='2020-01-01';

select ch.*,
cl.Email_OperationalOptIn, 
cl.Email_MarketingOptIn,
if(cl.Email_OperationalOptIn=1,1,0) as is_transactional_optin_email,
if(cl.Email_MarketingOptIn=1,1,0) as is_marketing_optin_email
from reporting.campaign_history ch 
inner join ais.vw_client cl on cl.id=ch.lms_customer_id and ch.lms_code='EPIC'
where ch.business_date>='2019-12-01'  LIMIT 100;



Update reporting.campaign_history ch 
inner join ais.vw_client cl on cl.id=ch.lms_customer_id and ch.lms_code='EPIC'  
set ch.is_transactional_optin=if(cl.Email_OperationalOptIn=1,1,0),
    ch.is_marketing_optin_email=if(cl.Email_MarketingOptIn=1,1,0)
where ch.business_date=curdate();



 
 

##TDC: LOC_001.ca_Customer_Flags cf on cf.Cust_ID=la.lms_customer_id - -  TRAN customer flags: 2-'Do Not Email' flag, 6-'Opt-Out Marketing', ref: LOC_001.ca_Merchant_Flags
select * from LOC_001.ca_Customer_Flags /*where Flag_ID=2 and Flag_Value=1*/ limit 1000;

Update reporting.loc_gc_campaign_history ch 
inner join LOC_001.ca_Customer_Flags cf on cf.Cust_ID=la.lms_customer_id and ch.job_ID like 'LOC%' 
set ch.is_transactional_optin=if(cf.Flag_ID =2 and cf.Flag_Value=1,0,1),
    ch.is_marketing_optin_email=if(cf.Flag_ID in(2,6) and cf.Flag_Value=1,0,1)
where ch.business_date=curdate();


select ch.*,
cf.Flag_ID,
cf.Flag_Value,
if(cf.Flag_ID=2 and cf.Flag_Value=1,0,1) as is_transactional_optin_email,
if(cf.Flag_ID in(2,6) and cf.Flag_Value=1,0,1) as is_marketing_optin_email
from reporting.loc_gc_campaign_history ch 
inner join LOC_001.ca_Customer_Flags cf on cf.Cust_ID=ch.lms_customer_id and ch.job_ID like 'LOC%' 
where ch.list_generation_date>='2018-12-01' and cf.Flag_ID=6  LIMIT 1000;

select distinct list_module from reporting.loc_gc_campaign_history;







####################################



-- reporting.SP_campaign_history_update
/*
Purpose of this SP:
1. To update JAG,EPIC and TDC's cellphone and homephone
2. To update JAG,EPIC and TDC's SMS's opt-in and opt-out flag
3. To update JAG,EPIC and TDC's email's opt-in and opt-out flag

To-do
1. change the column names on reporting.campaign_history and reporting.loc_gc_campaign_history for those 4 flags (Is_transactional_optin_SMS,Is_marketing_optin_SMS,Is_transactional_optin_email,Is_marketing_optin_email)
2. add column 'lms_code','home_phone' and 'cell_phone' on reporting.loc_gc_campaign_history
*/




  SET @valuation_date = curdate(); 

  

			SET @process_label ='populate the EPIC phone number into campaign_history', @process_type = 'update';
			
			update reporting.campaign_history ch
			inner join ais.vw_client vc on ch.lms_customer_id=vc.Id
            set ch.home_phone = vc.HomePhone, ch.cell_phone = vc.CellPhone 
			where ch.lms_code = 'EPIC' and ch.business_date >= @valuation_date;
			
		

			SET @process_label ='populate the TDC phone number into campaign_history', @process_type = 'update';
			
			update reporting.campaign_history ch
			inner join LOC_001.ca_Customer tc on ch.lms_customer_id= tc.Cust_ID
            set ch.home_phone = tc.Cust_HPhone, ch.cell_phone = tc.Cust_Mphone
			where ch.lms_code = 'TDC' and ch.business_date >= @valuation_date;
 


  
  
			SET @process_label ='populate the JAG phone number into campaign_history', @process_type = 'update';
			
			update reporting.campaign_history ch
			inner join jaglms.lms_customer_info_flat c on ch.lms_customer_id= c.customer_id
            set ch.home_phone = c.homephone, ch.cell_phone = c.cellphone
			where ch.lms_code = 'JAG' and ch.business_date >= @valuation_date;
 
 
 		update reporting.loc_gc_campaign_history ch
			inner join jaglms.lms_customer_info_flat c on ch.lms_customer_id= c.customer_id
            set ch.home_phone = c.homephone, 
            ch.cell_phone = c.cellphone
			where ch.job_ID like '%JAGLOC%' and ch.business_date >= @valuation_date;
 
 update reporting.monthly_campaign_history ch
			inner join jaglms.lms_customer_info_flat c on ch.lms_customer_id= c.customer_id
            set ch.home_phone = c.homephone, 
            ch.cell_phone = c.cellphone
			where ch.lms_code = 'JAG'  and date(ch.list_generation_time) >= @valuation_date;
 
      
   -- populate the Is_Transactional_optin and Is_SMS_Marketing_optin flag

			SET @process_label ='populate the SMS flag into campaign_history', @process_type = 'Update';   

      update reporting.campaign_history ch
		inner join (select list.customer_id, 
                count(if(list.notification_name = 'SMS_TRANSACTIONAL' and list.state = 1, list.customer_id, null)) as 'Transactional With Consent',
                count(if(list.notification_name = 'SMS_TRANSACTIONAL' and list.Txt_Stop = 1, list.customer_id, null)) as 'Transactional Text Stop',   
                count(if(list.notification_name = 'SMS_MARKETING' and list.state = 1, list.customer_id, null)) as 'SMS Marketing With Consent', 
                count(if(list.notification_name = 'SMS_MARKETING' and list.Txt_Stop = 1, list.customer_id, null)) as 'SMS Marketing Text Stop'
        from
              (select cn.customer_id,
                     nnm.notification_name, 
                     nnm.notification_name_mapping_id,
                     cn.first_consent_date_time,
                     cn.second_consent_date_time,
                     cn.state,
                    (select (case when s.message = 'Stop' then 1 else 0 end) from jaglms.sms_event_logs s  
                     where s.customer_id = cn.customer_id and s.notification_name_id = nnm.notification_name_mapping_id order by s.event_date desc limit 1) as 'Txt_Stop'
               from jaglms.lms_customer_notifications cn
        		   inner join jaglms.lms_notification_name_mapping nnm on cn.notification_name_id = nnm.notification_name_mapping_id) list
        group by list.customer_id) aa on ch.lms_customer_id = aa.customer_id
	    set ch.is_transactional_optin = if(aa.`Transactional With Consent`=1 and aa.`Transactional Text Stop`=0,1,0),
            ch.is_sms_marketing_optin = if(aa.`SMS Marketing With Consent`=1 and  aa.`SMS Marketing Text Stop`=0,1,0)
			where ch.lms_code = 'JAG' and ch.business_date >= @valuation_date;
 
   




Update reporting.campaign_history ch 
inner join jaglms.lms_customer_info_flat lci on lci.customer_id=ch.lms_customer_id
set ch.is_email_transactional_optin=if(lci.optout_account_email='false' or lci.optout_account_email is NULL,1,0),
    ch.is_email_marketing_optin=if(lci.optout_marketing_email='false' or lci.optout_marketing_email is NULL,1,0)
where ch.business_date>= @valuation_date  and ch.lms_code='JAG';


Update reporting.loc_gc_campaign_history ch 
inner join jaglms.lms_customer_info_flat lci on lci.customer_id=ch.lms_customer_id 
set ch.is_email_transactional_optin=if(lci.optout_account_email='false',1,0),
    ch.is_email_marketing_optin=if(lci.optout_marketing_email='false',1,0)
where ch.business_date>= @valuation_date and ch.job_ID like '%JAGLOC%';

Update reporting.monthly_campaign_history ch 
inner join jaglms.lms_customer_info_flat lci on lci.customer_id=ch.lms_customer_id
set ch.is_email_transactional_optin=if(lci.optout_account_email='false' or lci.optout_account_email is NULL,1,0),
    ch.is_email_marketing_optin=if(lci.optout_marketing_email='false' or lci.optout_marketing_email is NULL,1,0)
where date(ch.list_generation_time) >= @valuation_date  and ch.lms_code='JAG';



Update reporting.campaign_history ch 
inner join ais.vw_client cl on cl.id=ch.lms_customer_id
set ch.is_email_transactional_optin=if(cl.Email_OperationalOptIn=1,1,0),
    ch.is_email_marketing_optin=if(cl.Email_MarketingOptIn=1,1,0)
where ch.business_date>= @valuation_date  and ch.lms_code='EPIC';


Update reporting.monthly_campaign_history ch 
inner join ais.vw_client cl on cl.id=ch.lms_customer_id
set ch.is_email_transactional_optin=if(cl.Email_OperationalOptIn=1,1,0),
    ch.is_email_marketing_optin=if(cl.Email_MarketingOptIn=1,1,0)
where date(ch.list_generation_time) >= @valuation_date  and ch.lms_code='EPIC';


Update reporting.loc_gc_campaign_history ch 
inner join LOC_001.ca_Customer_Flags cf on cf.Cust_ID=la.lms_customer_id 
set ch.is_email_transactional_optin=if(cf.Flag_ID =2 and cf.Flag_Value=1,0,1),
    ch.is_email_marketing_optin=if(cf.Flag_ID in(2,6) and cf.Flag_Value=1,0,1)
where ch.business_date>= @valuation_date and ch.job_ID like 'LOC%';

Update reporting.campaign_history ch 
inner join LOC_001.ca_Customer_Flags cf on cf.Cust_ID=la.lms_customer_id 
set ch.is_email_transactional_optin=if(cf.Flag_ID =2 and cf.Flag_Value=1,0,1),
    ch.is_email_marketing_optin=if(cf.Flag_ID in(2,6) and cf.Flag_Value=1,0,1)
where ch.business_date>= @valuation_date and ch.lms_code='TDC';


