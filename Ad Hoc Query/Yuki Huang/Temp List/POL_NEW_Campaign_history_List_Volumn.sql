/* In Campaign history table, use state, product, and days after paid off to pull the POL_NEWL from Jun to Sep:
CA SEP: (10,30,40,50,60,75, 90)
AL SEP: (3, 20,30,40,50,60,75, 90)
CA PD, DE, IL, NM, UT, WI, MO, MS, ID, & TX: 3, 7, 10, 13, 15, 19, 23, 26, 30, 45, 60, 75 */

##############################################################################################



##CA SEP: (10,30,40,50,60,75, 90)
select month(list_generation_time),ch.business_date, ch.list_module, 
ch.lms_customer_id, ch.lms_application_id,
ch.lms_code, ch.state, ch.product, ch.Customer_FirstName, 
ch.Customer_LastName,
datediff(ch.list_generation_time,ch.last_repayment_date) as days_since_paidoff,
concat_ws('_',list_module,datediff(ch.list_generation_time,ch.last_repayment_date)) as List_name,
'CA_SEP' as Group_filter
from reporting.campaign_history ch 
where ch.list_module='POL_NEW' and ch.state='CA'
and datediff(ch.list_generation_time,ch.last_repayment_date) in (10,30,40,50,60,75,90)
and ch.list_generation_time between '2019-06-01' and '2019-09-31'
and ch.product ='SEP'

UNION



##AL SEP: (3, 20,30,40,50,60,75, 90)
select month(list_generation_time),ch.business_date, ch.list_module, 
ch.lms_customer_id, ch.lms_application_id,
ch.lms_code, ch.state, ch.product, ch.Customer_FirstName, 
ch.Customer_LastName,
datediff(ch.list_generation_time,ch.last_repayment_date) as days_since_paidoff,
concat_ws('_',list_module,datediff(ch.list_generation_time,ch.last_repayment_date)) as List_name,
'AL_SEP' as Group_filter
from reporting.campaign_history ch 
where ch.list_module='POL_NEW' and ch.state='AL'
and datediff(ch.list_generation_time,ch.last_repayment_date) in (3, 20,30,40,50,60,75,90)
and ch.list_generation_time >= '2019-06-01' and ch.list_generation_time <= '2019-9-30'
and ch.product ='SEP'

UNION


##CA PD, DE, IL, NM, UT, WI, MO, MS, ID, & TX: 3, 7, 10, 13, 15, 19, 23, 26, 30, 45, 60, 75 */
select month(list_generation_time),ch.business_date, ch.list_module, 
ch.lms_customer_id, ch.lms_application_id,
ch.lms_code, ch.state, ch.product, ch.Customer_FirstName, 
ch.Customer_LastName,
datediff(ch.list_generation_time,ch.last_repayment_date) as days_since_paidoff,
concat_ws('_',list_module,datediff(ch.list_generation_time,ch.last_repayment_date)) as List_name,
concat_ws('_',state,product) as Group_filter
from reporting.campaign_history ch 
where ch.list_module='POL_NEW' and ch.state in ('CA','DE','IL','NM', 'UT','WI','MO','MS','ID','TX')
and datediff(ch.list_generation_time,ch.last_repayment_date) in (3, 7, 10, 13, 15, 19, 23, 26, 30, 45, 60,75)
and ch.list_generation_time between '2019-06-01' and '2019-09-30'
and ch.product !='LOC';


