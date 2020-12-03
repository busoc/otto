create or replace view jobs_status (label, timestamp, count) as
with
	pending(workflow) as (select min(workflow) from replay_status),
    cancelled(workflow) as (select max(workflow) from replay_status),
    done(workflow) as (select workflow from replay_status order by workflow desc limit 4),
    completed(workflow) as (select workflow from replay_status order by workflow desc limit 4 offset 1),
    running(workflow) as (select workflow from replay_status where workflow <> (select workflow from pending) and workflow not in (select workflow from done))
select 'PENDING' as label, date(timestamp) as time, count(replay_id) as total
from replay_job where replay_status_id=(select id from replay_status where workflow=(select workflow from pending)) group by time
union all
select 'CANCELLED' as label, date(timestamp) as time, count(replay_id) as total
from replay_job where replay_status_id=(select id from replay_status where workflow=(select workflow from cancelled)) group by time
union all
select 'COMPLETED' as label, date(timestamp) as time, count(replay_id) as total
from replay_job where replay_status_id in (select id from replay_status where workflow in (select workflow from completed)) group by time
union all
select 'RUNNING' as label, date(timestamp) as time, count(replay_id) as total
from replay_job where replay_status_id in (select id from replay_status where workflow in (select workflow from running)) group by time
