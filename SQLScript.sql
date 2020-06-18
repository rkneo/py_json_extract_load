.header on
select 	ticket_id,
		sum(case when status = "Open" then 1 else 0 end) "Time spent Open", 
		sum(case when status = "Waiting for Customer" then 1 else 0 end)  "Time spent Waiting on Customer", 
		sum(case when status = "Pending" then 1 else 0 end) "Time spent waiting for response",
		sum(case when status = "Resolved" then 1 else 0 end) "Time till resolution",
		sum(case when status = "Closed" then 1 else 0 end) "Time to first response"
from 
activities_data ad
join 
activity a on ad.activities_skey = a.activities_skey
and ad.unqiue_load_id = a.unqiue_load_id
where status is not null
group by ticket_id