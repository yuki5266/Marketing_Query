
select aa.*
from ( select loan_header_id, customer_id, 
first_name, last_name, 
lms_entity_id,status, state,entity_name,  
case 
when entity_name in('Missouri Monthly','Missouri Non Monthly') then 'MO'
when entity_name in('Mississippi B/S/W','Mississippi M') then 'MS'
else left(entity_name,2)
end as entity_state,
if(case 
when entity_name in('Missouri Monthly','Missouri Non Monthly') then 'MO'
when entity_name in('Mississippi B/S/W','Mississippi M') then 'MS'
else left(entity_name,2)
end=state,1,0) as is_same,
created_date,
last_update
from jaglms.lms_loan_header where entity_name is not null and entity_name not in('CBW Non Monthly','CBW Monthly')) aa
where aa.is_same=0;