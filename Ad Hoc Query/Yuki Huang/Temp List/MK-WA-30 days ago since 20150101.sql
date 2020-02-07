/*
Starting Oct 23, we would run a weekly email/sms campaign for previously withdrawn customers. 
Please create a store procedure to generate a list of withdrawn customers who withdrew more than 30 days ago but after 1st Jan 2015.


select * from reporting.withdrawn_reason_code where withdrawn_reason in(
'Amount Too Low',
'Application Expired-3days', = auto-withdrawn
'Bank Account Activity',
'Bank Duration Too Short',
'Duplicate Application',
'Writing New Application',
'Fees Too High',
'Invalid Employment Type',
'Invalid Pay Frequency',
'Manual Application Expired',
'No Longer Interested',
'Refused to Esign',
'Refused to Provide Reference',
'Unable to Contact',
'Wants Money Today',
'Refused to use English in finalizing the Loan');


Fields to include
First Name
Email
Withdrawn Date 
State
Loan Amount --? Previous request loan amount

Max amount for the state  --? LLT or use the previous calculation? 
select cll.*,
(select value from jaglms.lms_entity_parameters lep where parameter_name = 'entity_name' and lep.lms_entity_id = cll.entity_id limit 1) as Entity_Name
from shared.credit_limit_lookup cll;

Product
Phone number
Marketing opt-in
*/





		set
		@channel='email',
		@list_name='WA_30days',
		@list_module='WA',
		@list_frq='W',
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
      la.approved_amount, -- DAT-983
      -- la.HardCap,
      la.storename,
      la.withdrawn_reason_code,
			la.withdrawn_reason,
      la.withdrawn_time, -- DAT-579
			if(lms_code='JAG' AND state not in ('TX', 'OH'),'Installment Loan application ',
			if(lms_code='TDC', 'Line of Credit application','application')) as key_word,
			@list_gen_time
     
			from reporting.leads_accepted la
			where
				SUBSTR(SUBSTR(emailaddress, INSTR(emailaddress, '@'), INSTR(emailaddress, '.')), 2) not in ('epic.lmsmail.com', 'moneykey.com')
			and SUBSTR(SUBSTR(emailaddress, INSTR(emailaddress, '@'), INSTR(emailaddress, '.')), 2) not like 'epic%'
			and la.IsApplicationTest = 0 
			and la.application_status in ('Withdrawn', 'Withdraw')
      and la.state in ('DE','IL', 'KS','MO','NM','TX','UT', 'SC', 'CA','AL', 'MS','ID','WI','TN')
			and withdrawn_reason_code in (1,2,4,5,8,9,10,12,13,16,19,24,25,26,27,32)
			and la.loan_sequence=1  
      and la.isreturning = 0 
			and date(la.withdrawn_time) between @std_date and @end_date
      and la.state !='OH'
			and IF(@opt_out_YN=1, la.Email_MarketingOptIn=1, la.Email_MarketingOptIn IN (1, 0))
			and la.MaxLoanLimit>0);
			-- and la.lms_customer_id not in(select distinct la2.lms_customer_id from reporting.leads_accepted la2 where la2.application_status='pending' or la2.origination_time is not null ));

	-- SELECT *FROM raw1 where emailaddress='COURTNEY.BRENT12@YAHOO.COM';

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
	 
--  select * from raw2 where emailaddress='COURTNEY.BRENT12@YAHOO.COM';
		
           
           
  Drop TEMPORARY table if exists raw3;
			Create TEMPORARY table if not exists raw3  (index (lms_customer_id,last_withdrawn_time)) 
      as( select r1.*,
      r2.last_withdrawn_time,
      datediff(CURDATE(),r2.last_withdrawn_time) as days_since_last_withdraw   
      from raw1 r1 
      inner join raw2 r2 on r1.lms_code=r2.lms_code and r1.lms_customer_id=r2.lms_customer_id and r1.withdrawn_time=r2.last_withdrawn_time    
      
);
      
    -- select count(*) from raw3;
        
           
           
 ## To remove any customer who has loan either with JAG,EPIC or TDC
     DROP TEMPORARY TABLE IF EXISTS exc1;
CREATE TEMPORARY TABLE IF NOT EXISTS exc1 ( INDEX (emailaddress) ) 
AS (
select distinct t1.emailaddress from raw3 t1 
join reporting.leads_accepted la2 on t1.emailaddress=la2.emailaddress -- and t1.lms_code <>la2.lms_code 
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



       -- select r5.*, count(emailaddress) as cnt from raw5 r5 group by emailaddress having cnt >1;
       -- select r5.* from raw4 r5 where emailaddress='crum826@gmail.com';
   -- select count(*) from raw4;
            
-- select * from exc1;           
-- select e.*, avg(la.loan_sequence) as avg1,sum(la.isoriginated) as originated1 from exc1 e left join reporting.leads_accepted la on la.emailaddress=e.emailaddress group by e.emailaddress;
-- select * from reporting.leads_accepted where emailaddress='crum826@gmail.com';

   

           
           
   Drop TEMPORARY table if exists e1;
			Create TEMPORARY table if not exists e1 
      as(
select
cll.*,
(select value from jaglms.lms_entity_parameters lep where parameter_name = 'entity_name' and lep.lms_entity_id = cll.entity_id limit 1) as Entity_Name
from shared.credit_limit_lookup cll);

-- select * from e1;



   Drop TEMPORARY table if exists e4;
			Create TEMPORARY table if not exists e4 (index (State))
      as(select e1.*,
      case when e1.Entity_Name like '%Idaho%' then 'ID'
           when e1.Entity_Name like '%Mississippi%' then 'MS'
           when e1.Entity_Name like '%Missouri%' then 'MO'
           When e1.Entity_Name like '%CBW%' then null
           else left(e1.Entity_Name,2)
           end as State from e1 e1);

-- select * from e4;


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
      
     --  select * from e5;




      
           
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
      where r1.days_since_last_withdraw>30);
      
  
    -- select * from table2;
    
    
    
       

    
    
    
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
    
  /*  
    select *,count(emailaddress) as cnt from table3 group by emailaddress having cnt>1;
    select *  from table3 where emailaddress='007larrytaylor@gmail.com';
    select count(*) from table3;
    select * from reporting.leads_accepted where emailaddress='RITH36.RT@GMAIL.COM';
   
select t3.emailaddress from table3 t3 join reporting.leads_accepted la on la.emailaddress=t3.emailaddress and la.origination_time>0 where t3.emailaddress !='';

    select * from table3;

    */
    
    
   

			/*-- INSERT INTO reporting.campaign_history

			(business_date, Channel,       list_name,      job_ID, list_module,    list_frq,    
      lms_customer_id,        lms_application_id, received_time,      lms_code,      
      state,  product,        loan_sequence,  email, 
      Customer_FirstName,
			Customer_LastName,      /*Req_Loan_Amount,approved_amount, Max_Loan_Limit,
      withdrawn_reason, withdrawn_time,list_generation_time, Is_Transactional_optin,Is_SMS_Marketing_optin) */


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
      -- t2.Req_Loan_Amount,
      t2.approved_amount, 
      t2.maximum_limit_byState,
      t2.withdrawn_reason1, 
      t2.withdrawn_time,
      t2.last_withdrawn_time,
      date(t2.last_withdrawn_time) as last_withdrawn_date,
      @list_gen_time,
      t2.cell_phone as Final_cell_phone,
      t2.Is_Transactional_optin,
      t2.Is_SMS_Marketing_optin,
      t2.days_since_last_withdraw,
      t2.group_filer,
      t2.IsValidEmail
      from table3 t2
      where t2.group_filer='Group1';
  
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
      -- t2.Req_Loan_Amount,
      t2.approved_amount, 
      t2.maximum_limit_byState,
      t2.withdrawn_reason1, 
      t2.withdrawn_time,
      t2.last_withdrawn_time,
      date(t2.last_withdrawn_time) as last_withdrawn_date,
      @list_gen_time,
      t2.cell_phone as Final_cell_phone,
      t2.Is_Transactional_optin,
      t2.Is_SMS_Marketing_optin,
      t2.days_since_last_withdraw,
      t2.group_filer,
      t2.IsValidEmail
      from table3 t2
      where t2.group_filer='Group2';
  

















/*
######################

select * from e3 group by entity_name,pay_frequency;
      
select lms_code,state, product,count(*) from reporting.leads_accepted group by state,product;


 ,
      
      Case 
when la.state='AL' and la.pay_frequency='M' and la.product in('FP','SEP') then 'AL 18M M'
when la.state='AL' and la.pay_frequency in('B','S','W') and la.product in('FP','SEP') then 'AL 18 Month B/S/W'

when la.state='CA' and la.pay_frequency='M' and la.product in ('FP','SEP') then 'CA 18M M'
when la.state='CA' and la.pay_frequency in('B','S','W') and la.product in ('FP','SEP') then 'CA 18 Month B/S/W'

when la.state='CA' and la.pay_frequency in('B','S','W','M') and la.product ='PD' then 'CA Payday'

-- CBW Monthly
-- CBW Non Monthly


when la.state='DE' and la.pay_frequency='M' and la.product='SEP' then 'DE SEP (Monthly)'
when la.state='DE' and la.pay_frequency in('B','S','W') and la.product='SEP' then 'DE SEP (NC) B/S/W'


when la.state='ID' and la.pay_frequency='M' and la.product IN('FP','IPP','SEP','PD') then 'Idaho Monthly'
when la.state='ID' and la.pay_frequency in('B','S','W') and la.product IN('FP','IPP','SEP','PD') then 'Idaho Non Monthly'


when la.state='IL' and la.pay_frequency='M' and la.product='SEP' then 'IL SEP M'
when la.state='IL' and la.pay_frequency in('B','S','W') and la.product='SEP' then 'IL SEP B/S/W'

-- IL SEP B/S/W 8 Pay

when la.state='KS' and la.pay_frequency='M' and la.product='LOC' then 'KS LOC2 M'
when la.state='KS' and la.pay_frequency in('B','S','W') and la.product='LOC' then 'KS LOC2 BSW'

when la.state='MS' and la.pay_frequency='M' and la.product IN ('FP','IPP','SEP','PD') then 'Mississippi M'
when la.state='MS' and la.pay_frequency in('B','S','W') and la.product IN ('FP','IPP','SEP','PD') then 'Mississippi B/S/W'

when la.state='MO' and la.pay_frequency='M' and la.product in ('FP','IPP','SEP','PD') then 'Missouri Monthly'  
when la.state='MO' and la.pay_frequency in('B','S','W') and la.product in ('FP','IPP','SEP','PD') then 'Missouri Non Monthly'

when la.state='NM' and la.pay_frequency='M' and la.product in ('FP','SEP','SP') then 'NM 2 Monthly'  
when la.state='NM' and la.pay_frequency in('B','S','W') and la.product in ('FP','SEP','SP') then 'NM 2 B/S/W'

when la.state='SC' and la.pay_frequency='M' and la.product='LOC' then 'SC LOC M'  
when la.state='SC' and la.pay_frequency in('B','S','W') and la.product='LOC' then 'SC LOC BSW'

when la.state='TN' and la.pay_frequency='M' and la.product='LOC' then 'TN LOC M'  
when la.state='TN' and la.pay_frequency in('B','S','W') and la.product='LOC' then 'TN LOC BSW'

when la.state='TX' and la.pay_frequency='M' and la.product in ('FP','SEP','IPP')and la.storename like('TX Install BAS%') then 'TX Install BAS M'
when la.state='TX' and la.pay_frequency in('B','S','W') and la.product in ('FP','SEP','IPP') and la.storename like('TX Install BAS%')  then 'TX Install BAS NM 2'


when la.state='TX' and la.pay_frequency='M' and la.product in ('FP','SEP','IPP') and la.storename like('TX Install NCP%') then 'TX Install NCP M'
when la.state='TX' and la.pay_frequency in('B','S','W') and la.product in ('FP','SEP','IPP')and la.storename like('TX Install NCP%')  then 'TX Install NCP NM'


when la.state='UT' and la.pay_frequency='M' and la.product in ('FP','SEP','SP') then 'UT Monthly'  
when la.state='UT' and la.pay_frequency in('B','S','W') and la.product in ('FP','SEP','SP') then 'UT SEP (NC) B/S/W'

when la.state='WI' and la.pay_frequency='M' and la.product in ('FP','SEP','SP') then 'WI Monthly'  
when la.state='WI' and la.pay_frequency in('B','S','W') and la.product in ('FP','SEP','SP') then 'WI Non-Monthly'
end as Test_store_name


*/



           
   Drop TEMPORARY table if exists e1;
			Create TEMPORARY table if not exists e1 
      as(
select
cll.*,
(select value from jaglms.lms_entity_parameters lep where parameter_name = 'entity_name' and lep.lms_entity_id = cll.entity_id limit 1) as Entity_Name
from shared.credit_limit_lookup cll);

-- select * from e1;



   Drop TEMPORARY table if exists e4;
			Create TEMPORARY table if not exists e4 
      as(select e1.*,
      case when e1.Entity_Name like '%Idaho%' then 'ID'
           when e1.Entity_Name like '%Mississippi%' then 'MI'
           when e1.Entity_Name like '%Missouri%' then 'MO'
           When e1.Entity_Name like '%CBW%' then null
           else left(e1.Entity_Name,2)
           end as State from e1 e1);

-- select * from e4;


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
      
     --  select * from e5;
     

/*
    Drop TEMPORARY table if exists e1;
			Create TEMPORARY table if not exists e1 
      as(
select cll.*,
(select value from jaglms.lms_entity_parameters lep where parameter_name = 'entity_name' and lep.lms_entity_id = cll.entity_id limit 1) as Entity_Name
from shared.credit_limit_lookup cll);

-- select * from e1;

  Drop TEMPORARY table if exists e2;
			Create TEMPORARY table if not exists e2 
      as(
      select e1.*,max(effective_date) as lastest_effective_date from e1
      group by e1.Entity_Name, e1.pay_frequency);
      
      
      -- select * from e2;
      
        Drop TEMPORARY table if exists e3;
			Create TEMPORARY table if not exists e3 
      as( select e1.* from e1 e1 inner join e2 e2 on e2.entity_id=e1.entity_id and e2.pay_frequency=e1.pay_frequency and e2.lastest_effective_date=e1.effective_date);
      
      
      -- select * from e3; */