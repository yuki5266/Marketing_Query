
		set
		@channel='email',
		@list_name='WA_30days',
		@list_module='WA-PRJ',
		@list_frq='D',
		@list_gen_time=now(),
		@time_filter='withdrawn_time',
		@opt_out_YN= 1,
    @valuation_date =curdate();
 

	 set
		@std_date= '2015-01-01',
		@end_date= curdate();

	
			Drop TEMPORARY table if exists raw1;
			Create TEMPORARY table if not exists raw1 (index (lms_customer_id, received_time,withdrawn_reason_code))
      as(
			select  distinct
      @valuation_date,
			@channel,
			@list_name,
			case
			when la.lms_code ='JAG' then date_format(@list_gen_time, '%m%d%YWAJ')
			else date_format(@list_gen_time, '%m%d%YWA')
			end as job_ID, 
			@list_module,
			@list_frq,
			la.lms_customer_id,
			la.lms_application_id,
			la.received_time,
			la.lms_code,
			la.state,
			la.product,
			la.loan_sequence,
      la.application_status,
			la.emailaddress,
      la.pay_frequency,
			CONCAT(UCASE(SUBSTRING(la.customer_firstname, 1, 1)),LOWER(SUBSTRING(la.customer_firstname, 2))) as FirstName,
			CONCAT(UCASE(SUBSTRING(la.customer_lastname, 1, 1)),LOWER(SUBSTRING(la.customer_lastname, 2))) as LastName,
			ifnull(la.requested_amount, la.approved_amount) as Req_Loan_Amount, 
      la.approved_amount, 
      la.storename,
      la.withdrawn_reason_code,
			la.withdrawn_reason,
      la.withdrawn_time, 
			@list_gen_time
     
			from reporting.leads_accepted la
			where
				SUBSTR(SUBSTR(emailaddress, INSTR(emailaddress, '@'), INSTR(emailaddress, '.')), 2) not in ('epic.lmsmail.com', 'moneykey.com')
			and SUBSTR(SUBSTR(emailaddress, INSTR(emailaddress, '@'), INSTR(emailaddress, '.')), 2) not like 'epic%'
			and la.IsApplicationTest = 0 
			and la.application_status in ('Withdrawn', 'Withdraw')
      and la.state in ('DE','IL','KS','MO','NM','TX','UT', 'SC', 'CA','AL', 'MS','ID','WI') -- exclude TN
			and withdrawn_reason_code in (1,2,4,5,8,9,10,12,13,16,19,24,25,26,27,32)
			and la.loan_sequence=1  
      and la.isreturning = 0 
			and date(la.withdrawn_time) between @std_date and @end_date
      and la.state !='OH'
			and IF(@opt_out_YN=1, la.Email_MarketingOptIn=1, la.Email_MarketingOptIn IN (1, 0))
			and la.MaxLoanLimit>0);


			Drop TEMPORARY table if exists raw2;
			Create TEMPORARY table if not exists raw2  (index (lms_customer_id, last_withdrawn_time)) 
      as(
			select  
			r1.lms_customer_id,
      r1.emailaddress,
      r1.withdrawn_reason_code,
			max(r1.withdrawn_time) as last_withdrawn_time,
			r1.lms_code
			from raw1 r1
			group by lms_code, lms_customer_id
      );
	 

		
           
           
  Drop TEMPORARY table if exists raw3;
			Create TEMPORARY table if not exists raw3  (index (lms_customer_id,last_withdrawn_time)) 
      as( select r1.*,
      r2.last_withdrawn_time,
      datediff(@valuation_date,r2.last_withdrawn_time) as days_since_last_withdraw 
      from raw1 r1 
      inner join raw2 r2 on r1.lms_code=r2.lms_code and r1.lms_customer_id=r2.lms_customer_id and r1.withdrawn_time=r2.last_withdrawn_time    
      
);
      

        
           
           

     DROP TEMPORARY TABLE IF EXISTS exc1;
CREATE TEMPORARY TABLE IF NOT EXISTS exc1 ( INDEX (emailaddress) ) 
AS (
select distinct t1.emailaddress from raw3 t1 
join reporting.leads_accepted la2 on t1.emailaddress=la2.emailaddress
where la2.application_status='pending' or la2.origination_time is not null
);


DROP TEMPORARY TABLE IF EXISTS raw4;
CREATE TEMPORARY TABLE IF NOT EXISTS raw4 ( INDEX (emailaddress) ) 
AS (select r3.* from raw3 r3 left join exc1 e on e.emailaddress=r3.emailaddress where e.emailaddress is null);



          DROP TEMPORARY TABLE IF EXISTS exc2;
CREATE TEMPORARY TABLE IF NOT EXISTS exc2 ( INDEX (emailaddress) ) 
AS (

select distinct t1.emailaddress,la2.withdrawn_time,la2.lms_code from raw4 t1 
join reporting.leads_accepted la2 on t1.emailaddress=la2.emailaddress -- and t1.lms_code <>la2.lms_code 
where la2.withdrawn_time<t1.last_withdrawn_time
);



DROP TEMPORARY TABLE IF EXISTS raw5;
CREATE TEMPORARY TABLE IF NOT EXISTS raw5 ( INDEX (emailaddress) ) 
AS (select r4.* from raw4 r4 left join exc2 e2 on e2.emailaddress=r4.emailaddress and r4.last_withdrawn_time=e2.withdrawn_time where e2.emailaddress is null group by r4.emailaddress );



           
           
   Drop TEMPORARY table if exists e1;
			Create TEMPORARY table if not exists e1 
      as(
select
cll.*,
(select value from jaglms.lms_entity_parameters lep where parameter_name = 'entity_name' and lep.lms_entity_id = cll.entity_id limit 1) as Entity_Name
from shared.credit_limit_lookup cll);





   Drop TEMPORARY table if exists e4;
			Create TEMPORARY table if not exists e4 (index (State))
      as(select e1.*,
      case when e1.Entity_Name like '%Idaho%' then 'ID'
           when e1.Entity_Name like '%Mississippi%' then 'MS'
           when e1.Entity_Name like '%Missouri%' then 'MO'
           When e1.Entity_Name like '%CBW%' then null
           else left(e1.Entity_Name,2)
           end as State from e1 e1);




  Drop TEMPORARY table if exists e2;
			Create TEMPORARY table if not exists e2 
      as(
      select e1.entity_id,e1.Entity_Name,e1.pay_frequency,max(e1.effective_date) as lastest_effective_date
      from e4 e1
      group by e1.Entity_Name,pay_frequency);
      
      
      -- select * from e2;
      
      Drop TEMPORARY table if exists e3;
			Create TEMPORARY table if not exists e3 
      as( select e1.* from e4 e1 inner join e2 e2 on e2.entity_id=e1.entity_id and e2.pay_frequency=e1.pay_frequency and e2.lastest_effective_date=e1.effective_date);
      
      -- select * from e3;
      
      Drop TEMPORARY table if exists e5;
			Create TEMPORARY table if not exists e5 
      as( select e3.effective_date,e3.Entity_name,e3.state,max(maximum_limit) as maximum_limit_byState from e3 e3 group by state);
      





      
           
      Drop TEMPORARY table if exists table2;
			Create TEMPORARY table if not exists table2 as(
      select 
      r1.*,
      IF(r1.lms_code = 'JAG',
       (CASE
           WHEN ff.cellphone = 9999999999 THEN ff.homephone
           WHEN ff.cellphone = 0000000000 THEN ff.homephone
           WHEN ff.cellphone = " " THEN ff.homephone
           ELSE ff.cellphone
        END),
       (CASE
           WHEN tt.cellphone = '(999)999-9999' THEN tt.homephone
           WHEN tt.cellphone = " " THEN tt.homephone
           ELSE tt.cellphone
        END)) as cell_phone,
          e5.state as test_state,
          e5.maximum_limit_byState 
         ,wrc.withdrawn_reason as withdrawn_reason1
      from raw5 r1   
      left JOIN jaglms.lms_customer_info_flat ff ON r1.lms_customer_id = ff.customer_id AND r1.lms_code = 'JAG'
      left JOIN ais.vw_client tt ON r1.lms_customer_id = tt.id AND r1.lms_code = 'EPIC'
      left join e5 e5 on e5.state=r1.state
      left join reporting.withdrawn_reason_code wrc on wrc.withdrawn_reason_code = r1.withdrawn_reason_code
      where r1.days_since_last_withdraw >30);
      

    
    
    
       

    
    
    
 Drop TEMPORARY table if exists table3;
 Create TEMPORARY table if not exists table3 
 as(
     SELECT t2.*,
     if(t2.days_since_last_withdraw<=365,'Group1','Group2') as group_filer,
    
     if(marketing.`Transactional With Consent`=1 and  marketing.`Transactional Text Stop`=0,1,0) as Is_Transactional_optin,
     if(marketing.`SMS Marketing With Consent`=1 and  marketing.`SMS Marketing Text Stop`=0,1,0) as Is_SMS_Marketing_optin,
     IF(t2.emailaddress like '%com' or t2.emailaddress like '%ca' or t2.emailaddress like '%net' or t2.emailaddress like '%fr' 
     or t2.emailaddress like '%org' or t2.emailaddress like '%us' or t2.emailaddress like '%edu' or t2.emailaddress like '%info' 
or t2.emailaddress like '%biz' 
or t2.emailaddress like '%.co.uk' or t2.emailaddress like '%.cn', 1, 0) as IsValidEmail
     from table2 t2    
     left join
        (select list.customer_id, 
                max(list.first_consent_date_time) as first_consent_date_time,
                count(distinct if(list.notification_name = 'SMS_TRANSACTIONAL', list.customer_id, null)) as 'Is Customer for transactional',
                count(if(list.notification_name = 'SMS_TRANSACTIONAL' and list.state = 1, list.customer_id, null)) as 'Transactional With Consent',
                count(if(list.notification_name = 'SMS_TRANSACTIONAL' and (list.state=0 or list.state is null), list.customer_id, null)) as 'Transactional Without Consent',
                count(if(list.notification_name = 'SMS_TRANSACTIONAL' and list.Txt_Stop = 1, list.customer_id, null)) as 'Transactional Text Stop',     
                
                count(distinct if(list.notification_name = 'PHONE_MARKETING', list.customer_id, null)) as 'Is Customer for phone Marketing',
                count(if(list.notification_name = 'PHONE_MARKETING' and list.state = 1, list.customer_id, null)) as 'Phone Marketing With Consent',
                count(if(list.notification_name = 'PHONE_MARKETING' and (list.state=0 or list.state is null), list.customer_id, null)) as 'Phone Marketing Without Consent',
                count(if(list.notification_name = 'PHONE_MARKETING' and list.Txt_Stop = 1, list.customer_id, null)) as 'Phone Marketing Text Stop',
                
                count(distinct if(list.notification_name = 'SMS_MARKETING', list.customer_id, null)) as 'Is Customer for SMS Marketing',
                count(if(list.notification_name = 'SMS_MARKETING' and list.state = 1, list.customer_id, null)) as 'SMS Marketing With Consent',
                count(if(list.notification_name = 'SMS_MARKETING' and (list.state=0 or list.state is null), list.customer_id, null)) as 'SMS Marketing Without Consent',
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
        group by list.customer_id) marketing  
on t2.lms_customer_id=marketing.customer_id and t2.lms_code='JAG'

);



###GROUP1
INSERT INTO
 reporting.campaign_history(business_date, Channel, list_name, job_ID, list_module,
  list_frq, lms_customer_id, lms_application_id, received_time, lms_code, state, product,
   loan_sequence, email, Customer_FirstName, Customer_LastName, approved_amount, max_loan_limit,
    withdrawn_reason, withdrawn_time, list_generation_time, cell_phone, is_transactional_optin, is_sms_marketing_optin,
   extra1, key_word)

			select 
      @valuation_date,
      @Channel,
      @list_name,
      t2.Job_ID,
      @list_module,
      @list_frq,
      t2.lms_customer_id,
      t2.lms_application_id,
      t2.received_time,
      t2.lms_code,
      t2.state,
      t2.product,
      t2.loan_sequence,
      t2.emailaddress,
      t2.FirstName,
      t2.LastName,
      t2.approved_amount, 
      t2.maximum_limit_byState,
      t2.withdrawn_reason1, 
      t2.last_withdrawn_time,
      @list_gen_time,
      t2.cell_phone as Final_cell_phone,
      t2.Is_Transactional_optin,
      t2.Is_SMS_Marketing_optin,
      t2.days_since_last_withdraw,
      t2.group_filer
      from table3 t2
      where t2.group_filer='Group1' 
      and t2.IsValidEmail=1
      and 
       case 
      when WEEKDAY(@valuation_date)=0 then t2.state in ('WI', 'MO')
      when WEEKDAY(@valuation_date)=1 then t2.state in ('CA', 'NM')
      when WEEKDAY(@valuation_date)=2 then t2.state in ('KS', 'MS', 'IL')
      when WEEKDAY(@valuation_date)=3 then t2.state in ('ID','SC','UT','DE','AL')
      end;
      
      
      
       ##Group1: Jasper report query   
      select 
      business_date, Channel, list_name, job_ID, list_module,
  list_frq, lms_customer_id, lms_application_id, received_time, lms_code, state, product,
   case
      when product ='LOC' then 'Line Of Credit'
      when product in ('SEP','IPP','FP') then 'Installment Loan'
      when product in ('PD','SP') then 'Payday Loan'
      else product
      end as Product_fullname,
   loan_sequence, email, Customer_FirstName, Customer_LastName, approved_amount, max_loan_limit as maximum_limit_byState,
    withdrawn_reason, date(withdrawn_time) as last_withdrawn_date,list_generation_time,    
    IF(lms_code = 'JAG',
       (CASE
           WHEN cell_phone = 9999999999 THEN home_phone
           WHEN cell_phone = 0000000000 THEN home_phone
           WHEN cell_phone = " " THEN home_phone
           ELSE cell_phone
        END),
       (CASE
           WHEN cell_phone = '(999)999-9999' THEN home_phone
           WHEN cell_phone = " " THEN home_phone
           ELSE cell_phone
        END)) as cell_phone,
    is_transactional_optin, is_sms_marketing_optin,
   extra1 as days_since_last_withdraw,key_word as Group_Filter
   from reporting.campaign_history
   where list_module='WA-PRJ' and business_date=curdate() and key_word='Group1';