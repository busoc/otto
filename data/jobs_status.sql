create or replace view jobs_status (label, timestamp, count) as
with
	pending(workflow) as (select min(workflow) from replay_status),
  cancelled(workflow) as (select max(workflow) from replay_status),
  done(workflow) as (select workflow from replay_status order by workflow desc limit 4),
  completed(workflow) as (select workflow from replay_status order by workflow desc limit 4 offset 1),
  running(workflow) as (select workflow from replay_status where workflow <> (select workflow from pending) and workflow not in (select workflow from done))
select
	'PENDING' as label,
	date,
	count(replay) as total
from latest_status
where status=(select id from replay_status where workflow=(select workflow from pending))
group by date
union all
select
	'CANCELLED' as label,
	date,
	count(replay) as total
from latest_status
where status=(select id from replay_status where workflow=(select workflow from cancelled))
group by date
union all
select
	'COMPLETED' as label,
	date,
	count(replay) as total
from latest_status
where status in (select id from replay_status where workflow in (select workflow from completed))
group by date
union all
select
	'RUNNING' as label,
	date,
	count(replay) as total
from latest_status
where status in (select workflow from running)
group by date;
