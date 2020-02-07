
select ch.*,
n.campaign_name, n.provider_name, n.lms_code, n.product,
n.state, n.loan_sequence, n.store_name, n.customer_id,
n.original_lead_id, n.loan_application_id, n.origination_loan_id,
n.original_lead_received_date, n.is_originated,
n.application_status, n.current_loan_status, n.withdrawn_reason, n.withdrawn_reason_detail,n.origination_datetime, n.withdrawn_datetime,n.homephone,n.cellphone,
n.cnt_inbound_call_home, n.cnt_inbound_call_cell
from reporting.campaign_history ch
left join reporting.AFR_Normal n on ch.lms_code = n.lms_code and ch.lms_application_id=n.original_lead_id
where business_date='2020-01-08' and list_module in('PA','PA_RC','PA2','PA2_RC') and is_transactional_optin=1;



select * from reporting.campaign_history ch where ch.email='adoremesara@gmail.com';



left join jaglms.lms_customer_info_flat c on ch.lms_customer_id= c.customer_id
            set ch.home_phone = c.homephone, ch.cell_phone = c.cellphone





select * from phone_ops.Master_Call_Log where call_start_time>= '01/08/2020 12:30:00 PM' and called_ani=5129662857;


################################


select ch.*, 
n.original_lead_received_date,
n.origination_datetime,
if(sum(if(n.origination_datetime>0,1,0))>0,1,0) as Is_originated,
mc.called_ani,
min(mc.call_start_time) as first_called_time,
c.cellphone,
c.homephone,
if(mc.Id is not null and mc.call_start_time  between '2020-01-08 12:30:00' and '2020-01-09 12:30:00',1,0) as Is_called_in,
day(n.origination_datetime) as day_of_month_originated,
day(mc.call_start_time) as day_of_month_call_in
from reporting.campaign_history ch
left join reporting.AFR_Normal n on ch.lms_code = n.lms_code and ch.lms_application_id=n.original_lead_id and n.origination_datetime between '2020-01-08 12:30:00' and '2020-01-09 12:30:00' 
left join jaglms.lms_customer_info_flat c on ch.lms_customer_id= c.customer_id
left join phone_ops.Master_Call_Log mc on (mc.called_ani = (CASE
           WHEN c.cellphone = 9999999999 THEN c.homephone
           WHEN c.cellphone = 0000000000 THEN c.homephone
           WHEN c.cellphone = " " THEN c.homephone
           ELSE c.cellphone
        END))  and (date(mc.call_start_time) between '2020-01-08'  and '2020-01-09')
where date(ch.list_generation_time)='2020-01-08' and ch.list_module in('PA','PA_RC','PA2','PA2_RC') and ch.is_transactional_optin=1
group by ch.lms_application_id;

