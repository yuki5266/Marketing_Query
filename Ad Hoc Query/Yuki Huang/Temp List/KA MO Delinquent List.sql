##KS
select la.state,la.lms_customer_id, la.lms_application_id, la.original_lead_id, la.loan_status, bsp.DaysDelinquent,
        la.emailaddress,  
        concat(ucase(substring(la.customer_firstname,1,1)), lower(substring(la.customer_firstname,2)), ' ', 
               ucase(substring(la.customer_lastname,1,1)), lower(substring(la.customer_lastname,2))) as Customer_FullName,
        date(la.origination_time) as Origination_Date,
        bsp.CreditLimit as LOC_Amount,
        date_sub(curdate(), interval bsp.DaysDelinquent day) as Due_Date,
        date_add(curdate(), interval 21 day) as Cure_Date,
        sh.StatementDate, sh.MinimumPaymentDue as Minimum_Payment_Due,
        'Olivia Harper' as 'Agent Name',
        '4122' as 'Agent Extension',
        NOW() as 'List_Generation_date',
        dayname(CURDATE()),
        DATE_FORMAT(curdate(), "%W , %M %e , %Y")

from LOC_001.ca_BSegment_Primary bsp
inner join reporting.leads_accepted la on trim(leading 0 from bsp.AccountNumber)=la.lms_customer_id and 
                                          la.lms_code='TDC' and la.origination_time>0 and la.state='KS'
left join LOC_001.ca_StatementHeader sh on bsp.AccountNumber=sh.AccountNumber and date_sub(curdate(), interval bsp.DaysDelinquent day)
                                                                                  =date(sh.DateOfTotalDue)                                        
where bsp.SystemStatus=3 and bsp.CCinHParent125AID!=1002
      and (if(weekday(curdate())=0, bsp.DaysDelinquent in (11,12,13), bsp.DaysDelinquent=11))
      
##MO
union

select  la.state,la.lms_customer_id, la.original_lead_id as lms_application_id, /*la.lms_application_id,*/la.original_lead_id,la.loan_status, bsp.DaysDelinquent,
        la.emailaddress,  
        concat(ucase(substring(la.customer_firstname,1,1)), lower(substring(la.customer_firstname,2)), ' ', 
               ucase(substring(la.customer_lastname,1,1)), lower(substring(la.customer_lastname,2))) as Customer_FullName,
        date(la.origination_time) as Origination_Date,
        bsp.CreditLimit as LOC_Amount,
        date_sub(curdate(), interval bsp.DaysDelinquent day) as Due_Date,
        date_add(curdate(), interval 21 day) as Cure_Date,
        sh.StatementDate, sh.MinimumPaymentDue as Minimum_Payment_Due,
        'Olivia Harper' as 'Agent Name',
        '4122' as 'Agent Extension',
       NOW() as 'List_Generation_date',
        dayname(CURDATE()),
        DATE_FORMAT(curdate(), "%W , %M %e , %Y")

from LOC_001.ca_BSegment_Primary bsp
inner join reporting.leads_accepted la on trim(leading 0 from bsp.AccountNumber)=la.lms_customer_id and 
                                          la.lms_code='TDC' and la.origination_time>0 and la.state='MO'
left join LOC_001.ca_StatementHeader sh on bsp.AccountNumber=sh.AccountNumber and date_sub(curdate(), interval bsp.DaysDelinquent day)
                                                                                  =date(sh.DateOfTotalDue)                                        
where bsp.SystemStatus=3 and bsp.CCinHParent125AID!=1002
      and (if(weekday(curdate())=0, bsp.DaysDelinquent in (11,12,13), bsp.DaysDelinquent=11));
      
      
      
      
 ##################################run this one
      
      
      ##KS
SELECT la.state,
       la.lms_customer_id,
       la.lms_application_id,
       -- la.original_lead_id,
       la.loan_status,
       bsp.DaysDelinquent,
       la.emailaddress as email,
       concat(ucase(substring(la.customer_firstname, 1, 1)),
              lower(substring(la.customer_firstname, 2)),
              ' ',
              ucase(substring(la.customer_lastname, 1, 1)),
              lower(substring(la.customer_lastname, 2)))
          AS full_name,
       date(la.origination_time) AS Origination_Date,
       bsp.CreditLimit
          AS LOC_Amount,
       date_sub(curdate(), INTERVAL bsp.DaysDelinquent DAY)
          AS Due_Date,
       date_add(curdate(), INTERVAL 21 DAY)
          AS Cure_Date,
      date(sh.StatementDate) as statement_date,
       sh.MinimumPaymentDue
          AS Minimum_Payment_Due,
       'Olivia Harper'
          AS 'Agent Name',
       '4122'
          AS 'Agent Extension'
       -- NOW()AS 'List_Generation_date'
       -- dayname(CURDATE()),
       -- DATE_FORMAT(curdate(), "%W , %M %e , %Y")
FROM LOC_001.ca_BSegment_Primary bsp
     INNER JOIN reporting.leads_accepted la
        ON     trim(LEADING 0 FROM bsp.AccountNumber) = la.lms_customer_id
           AND la.lms_code = 'TDC'
           AND la.origination_time > 0
           AND la.state = 'KS'
     LEFT JOIN LOC_001.ca_StatementHeader sh
        ON     bsp.AccountNumber = sh.AccountNumber
           AND date_sub(curdate(), INTERVAL bsp.DaysDelinquent DAY) =
               date(sh.DateOfTotalDue)
WHERE     bsp.SystemStatus = 3
      AND bsp.CCinHParent125AID != 1002
      AND (if(weekday(curdate()) = 0,
              bsp.DaysDelinquent IN (11, 12, 13),
              bsp.DaysDelinquent = 11))
##MO
UNION
SELECT la.state,
       la.lms_customer_id,
       la.original_lead_id AS lms_application_id,                    /*la.lms_application_id,*/
       -- la.original_lead_id,
       la.loan_status,
       bsp.DaysDelinquent,
       la.emailaddress as email,
       concat(ucase(substring(la.customer_firstname, 1, 1)),
              lower(substring(la.customer_firstname, 2)),
              ' ',
              ucase(substring(la.customer_lastname, 1, 1)),
              lower(substring(la.customer_lastname, 2)))
          AS full_name,
       date(la.origination_time) AS Origination_Date,
       bsp.CreditLimit
          AS LOC_Amount,
       date_sub(curdate(), INTERVAL bsp.DaysDelinquent DAY)
          AS Due_Date,
       date_add(curdate(), INTERVAL 21 DAY)
          AS Cure_Date,
        date(sh.StatementDate) as statement_date,
       sh.MinimumPaymentDue
          AS Minimum_Payment_Due,
       'Olivia Harper'
          AS 'Agent Name',
       '4122'
          AS 'Agent Extension'
      --  NOW() AS 'List_Generation_date'
       -- dayname(CURDATE()),
       -- DATE_FORMAT(curdate(), "%W , %M %e , %Y")
FROM LOC_001.ca_BSegment_Primary bsp
     INNER JOIN reporting.leads_accepted la
        ON     trim(LEADING 0 FROM bsp.AccountNumber) = la.lms_customer_id
           AND la.lms_code = 'TDC'
           AND la.origination_time > 0
           AND la.state = 'MO'
     LEFT JOIN LOC_001.ca_StatementHeader sh
        ON     bsp.AccountNumber = sh.AccountNumber
           AND date_sub(curdate(), INTERVAL bsp.DaysDelinquent DAY) =
               date(sh.DateOfTotalDue)
WHERE     bsp.SystemStatus = 3
      AND bsp.CCinHParent125AID != 1002
      AND (if(weekday(curdate()) = 0,
              bsp.DaysDelinquent IN (11, 12, 13),
              bsp.DaysDelinquent = 11));

