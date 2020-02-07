
	DROP TEMPORARY TABLE IF EXISTS table1;
				CREATE TEMPORARY TABLE IF NOT EXISTS table1 
				AS (
select
ma.lead_sequence_id,
ma.insert_date,
ma.decision,
ma.decision_detail,
ma.campaign_name,
if(ma.decision='ACCEPT',1,0) as is_accept,
la.lms_code,
ifnull(la.state,ma.state) as state,
la.product,
if(la2.origination_time>0, la2.isoriginated,la.isoriginated) as Is_originated,
if(la2.origination_time>0, la2.approved_amount,null) as Origination_loan_amount,
if(la2.origination_time>0, la2.withdrawn_reason ,la.withdrawn_reason) as withdrawn_reason,
ma.email
-- la.lead_sequence_id
from datawork.mk_application ma
left join jaglms.lead_source ls on ls.master_source_id=ma.lead_source_id
left join reporting.leads_accepted la on ma.lead_sequence_id = la.lead_sequence_id and la.isuniqueaccept=1
left join reporting.leads_accepted la2 on la.lms_customer_id=la2.lms_customer_id and la.lms_code=la2.lms_code and la.origination_loan_id=la2.loan_number
where ma.lead_source_id=545 and ma.insert_date>='2019-10-29'
-- group by ma.lead_sequence_id,ma.email
);


select * from table1;

select * from jaglms.lead_source ;

select * from reporting.AFR_Normal where campaign_name='#MK-MOB-PRJ#';


DROP TEMPORARY TABLE IF EXISTS table2;
				CREATE TEMPORARY TABLE IF NOT EXISTS table2 
				AS (
        
        select 
        n.lms_code, n.product, n.state,
        n.provider_name, n.campaign_name,
        n.origination_datetime, n.is_originated,
        n.is_1st_payment_defaulted
        from reporting.AFR_Normal n 
        left join table1 t1 on t1.lead_sequence_id=n.lead_sequence_id);






