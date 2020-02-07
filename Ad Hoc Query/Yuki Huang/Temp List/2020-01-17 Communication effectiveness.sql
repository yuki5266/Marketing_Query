
/*
0=Mon
1=Tue
2=Wed
3=Thur
4=Fri
5=Sat
6=Sun


*/
select 
c.*,
DAYNAME(date(c.received_time)) as dayname_received,
weekday(c.received_time) as day_received,
hour(c.received_time) as hour_received,
case
when weekday(c.received_time) in (0,1,2,3,4) and hour(c.received_time) between 8 and 22 then 1 
when weekday(c.received_time) in (5,6) and hour(c.received_time) between 11 and 18 then 1
else 0
end as Is_CC_Open
from temp.Communication_import c;