select * from variable;

select
  r.id,
  r.timestamp,
  r.startdate,
  r.enddate,
  coalesce(r.priority, -1) as priority,
  coalesce(j.text, "") as comment,
  s.name as status,
  !isnull(g.replay_id) as automatic
from
  replay as r
  join replay_job as j on r.id=j.replay_id
  join replay_status as s on j.replay_status_id=s.id
  left outer join gap_replay_list g on r.id=g.replay_id

select
  id,
  timestamp,
  last_timestamp,
  last_sequence_count,
  next_timestamp,
  next_sequence_count,
  chanel
from hrd_packet_gap

select
  g.id,
  g.timestamp,
  g.last_timestamp,
  g.last_sequence_count,
  g.next_timestamp,
  g.next_sequence_count,
  r.source,
  r.phase
from
  vmu_packet_gap as g
  join vmu_record as r on g.vmu_record_id=r.id

SELECT
  r.id, r.timestamp, r.startdate, r.enddate, COALESCE(r.priority, -1), COALESCE(j.text, ''), s.name, ISNULL(g.replay_id)
FROM replay AS r
  INNER JOIN (SELECT id, replay_id, MAX(replay_status_id) AS replay_status_id, text FROM replay_job GROUP BY replay_id) AS j ON r.id = j.replay_id
  INNER JOIN replay_status AS s ON s.id = j.replay_status_id
  LEFT OUTER JOIN (SELECT DISTINCT replay_id FROM gap_replay_list) AS g ON r.id = g.replay_id

  select r.source, sum(g.total)
  from vmu_record as r
  join (select count(vmu_record_id) as total, vmu_record_id from vmu_packet_gap group by vmu_record_id) as g on r.id=g.vmu_record_id
  group by r.source

with
  completed(workflow) as (select workflow from replay_status order by workflow DESC LIMIT 3 OFFSET 1),
  pending(workflow) as (select min(workflow) from replay_status),
  cancelled(workflow) as (select max(workflow) from replay_status),
  running(workflow) as (select workflow from replay_status where workflow <> (select workflow from pending) and workflow not in (select workflow from completed))
select * from (select
	'PENDING', DATE(timestamp) as TS, count(replay_id) as STAT
from replay_job
where replay_status_id=(select id from replay_status where workflow=(select workflow from pending))
group by TS
UNION ALL
select
	'CANCELLED', DATE(timestamp) as TS, count(replay_id) as STAT
from replay_job
where replay_status_id=(select id from replay_status where workflow=(select workflow from cancelled))
group by TS
UNION ALL
select
	'COMPLETED', DATE(timestamp) as TS, count(replay_id) as STAT
from replay_job
where replay_status_id IN (select id from replay_status where workflow in (select workflow from completed))
group by TS
UNION ALL
  select 'RUNNING', TS, count(replay_id) as STAT
  from (
    select DATE(timestamp) as TS, replay_id
    from replay_job j inner join replay_status as s on j.replay_status_id=s.id
    where replay_status_id IN (select id from replay_status where workflow in (select workflow from running))
  ) as d group by TS
) as g where TS >= DATE_SUB(CURRENT_DATE, INTERVAL 15 DAY);
