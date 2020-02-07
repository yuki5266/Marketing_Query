/*
Update SP:reporting.SP_campaign_history_update
Purpose1: To update the SMS and Email opt-in and opt-out flag for the list generated on the same day after all other campaign list SPs 
Purpose2: To update the phone number for JAG,TDC and EPIC
*/



  SET @valuation_date = curdate(); 

############UPDATE phone number
			update reporting.campaign_history ch
			inner join ais.vw_client vc on ch.lms_customer_id=vc.Id
            set ch.home_phone = vc.HomePhone, ch.cell_phone = vc.CellPhone 
			where ch.lms_code = 'EPIC' and ch.business_date >= @valuation_date;
			

			update reporting.campaign_history ch
			inner join LOC_001.ca_Customer tc on ch.lms_customer_id= tc.Cust_ID
            set ch.home_phone = tc.Cust_HPhone, ch.cell_phone = tc.Cust_Mphone
			where ch.lms_code = 'TDC' and ch.business_date >= @valuation_date;
 

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
 
      
#########populate the Is_Transactional_optin and Is_SMS_Marketing_optin flag


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
 
   

##########populate the is_email_transactional_optin and is_email_marketing_optin flag


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


