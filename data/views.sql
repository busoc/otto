create or replace view apidaysback(day) as
	select ifnull((select value from variable where name='api_days_back' limit 1), 15);

create or replace view days_back(date) as
	select date_sub(current_date(), interval (select day from apidaysback) DAY);

create or replace view completed_workflows(wf) as
	select workflow from replay_status order by workflow desc limit 4;

create or replace view pending_workflow(wf) as
	select min(workflow) from replay_status;

create or replace view cancelled_workflow(wf) as
	select max(workflow) from replay_status;

create or replace view exited_workflows(wf) as
	select workflow from replay_status order by workflow desc limit 4 offset 1;

create or replace view running_workflows(wf) as
	select workflow
	from replay_status
	where workflow <> (select wf from pending_workflow)
		and workflow not in (select workflow from completed_workflows);

create or replace view latest_status(replay, date, status) as
  select
    replay_id,
    date(timestamp) as date,
    max(replay_status_id) as replay_status_id
  from replay_job
  where timestamp >= (select date from days_back)
  group by date, replay_id
  order by replay_id;

create or replace view completed_replays(id) as
	select
		replay_id
	from replay_job
	where replay_status_id in (
		select
			id
		from replay_status
		where workflow in (select wf from completed_workflows)
	);

create or replace view channel_infos(channel, total) as
  select
    chanel,
    count(chanel)
  from hrd_packet_gap
  where timestamp >= (select date from days_back)
  group by chanel;

create or replace view hrd_gap_list(id, timestamp, channel, last_sequence_count, last_timestamp, next_sequence_count, next_timestamp, corrupted, completed, replay) as
select
  h.id,
  h.timestamp,
  h.chanel,
  h.last_sequence_count,
  h.last_timestamp,
  h.next_sequence_count,
  h.next_timestamp,
  h.next_sequence_count=h.last_sequence_count,
	r.id is not null,
  i.replay_id
from hrd_packet_gap h
  join gap_replay_list i on i.hrd_packet_gap_id=h.id
  left outer join completed_replays r on r.id=i.replay_id
  where h.timestamp >= (select date from days_back);

create or replace view hrd_status_list(label, timestamp, channel, count) as
  select
    'CORRUPTED',
    date(timestamp) as date,
    channel,
    count(id)
  from hrd_gap_list
  where corrupted
  group by date, channel
  union all
  select
    'MISSING',
    date(timestamp) as date,
    channel,
    sum(next_sequence_count-last_sequence_count)
  from hrd_gap_list
  where not corrupted
  group by date, channel;

create or replace view items_count(label, origin, date, count, missing, duration) as
  select
    'REPLAY' as label,
    'ALL' as origin,
    date(timestamp) as date,
    count(id) as total,
    0,
    sum(unix_timestamp(enddate) - unix_timestamp(startdate)) as duration
  from replay
  where enddate > startdate
    and replay.timestamp >= (select date from days_back)
  group by date
  union all
  select
    'HRD' as label,
    chanel as origin,
    date(timestamp) as date,
    count(id) as total,
    sum(next_sequence_count-last_sequence_count),
    sum(unix_timestamp(next_timestamp) - unix_timestamp(last_timestamp)) as duration
  from hrd_packet_gap
  where next_timestamp > last_timestamp
    and hrd_packet_gap.timestamp >= (select date from days_back)
  group by date, chanel
  union all
  select
    'VMU' as label,
    r.source as origin,
    date(g.timestamp) as date,
    count(g.id) as total,
    sum(next_sequence_count-last_sequence_count),
    sum(unix_timestamp(next_timestamp) - unix_timestamp(last_timestamp)) as duration
  from vmu_packet_gap as g
    inner join vmu_record as r on g.vmu_record_id=r.id
  where next_timestamp > last_timestamp
    and g.timestamp >= (select date from days_back)
  group by date, r.source;

create or replace view jobs_status (label, timestamp, count) as
select
	'PENDING' as label,
	date,
	count(replay) as total
from latest_status
where status=(select id from replay_status where workflow=(select wf from pending_workflow))
group by date
union all
select
	'CANCELLED' as label,
	date,
	count(replay) as total
from latest_status
where status=(select id from replay_status where workflow=(select wf from cancelled_workflow))
group by date
union all
select
	'COMPLETED' as label,
	date,
	count(replay) as total
from latest_status
where status in (select id from replay_status where workflow in (select wf from exited_workflows))
group by date
union all
select
	'RUNNING' as label,
	date,
	count(replay) as total
from latest_status
where status in (select wf from running_workflows)
group by date;

create or replace view records_count(id, total) as
	select
		vmu_record_id,
		count(vmu_record_id)
	from vmu_packet_gap
	where timestamp >= (select date from days_back)
	group by vmu_record_id;

create or replace view source_infos(source, total) as
  select
    r.source,
    sum(g.total)
  from vmu_record r
  join records_count g on r.id=g.id
  where r.source is not null
  group by r.source;

create or replace view record_infos(phase, total) as
  select
    r.phase,
    sum(g.total)
  from vmu_record r
  join records_count g on r.id=g.id
  where r.phase is not null
  group by r.phase;

create or replace view corrupted_hrd_list(id, total) as
	select
		replay,
		count(id)
	from hrd_gap_list
	where corrupted and timestamp >= (select date from days_back)
	group by replay;

create or replace view missing_hrd_list(id, total) as
	select
		replay,
		sum(next_sequence_count-last_sequence_count)
	from hrd_gap_list
	where timestamp >= (select date from days_back)
	group by replay;

create or replace view replay_job_list(replay, text, status) as
	select
		j.replay_id,
		j.text,
		j.replay_status_id
	from replay_job j
	join latest_status s on j.replay_id=s.replay and j.replay_status_id=s.status
	where j.timestamp >= (select date from days_back);

create or replace view replay_list(id, timestamp, startdate, enddate, priority, comment, status, automatic, cancellable, corrupted, missing) as
	select
		r.id,
		r.timestamp,
		r.startdate,
		r.enddate,
		coalesce(r.priority, -1) as priority,
		coalesce(j.text, '') as comment,
		s.name,
		g.replay is not null as automatic,
		-- replay_status_id not in (select * from cancellable) as cancellable,
		s.workflow not in (select wf from completed_workflows) as cancellable,
	  coalesce(c.total, 0) as corrupted,
		coalesce(m.total, 0) as missing
	from replay as r
		inner join replay_job_list as j on r.id = j.replay
		inner join replay_status as s on s.id = j.status
		left outer join hrd_gap_list as g on r.id=g.replay
	  left outer join corrupted_hrd_list as c on c.id=r.id
		left outer join missing_hrd_list as m on m.id=r.id
		where r.timestamp >= (select date from days_back);

create or replace view vmu_gap_list(id, timestamp, last_sequence_count, last_timestamp, next_sequence_count, next_timestamp, source, phase, corrupted, replay, completed) as
select
	g.id,
  g.timestamp,
  g.last_sequence_count,
  g.last_timestamp,
  g.next_sequence_count,
  g.next_timestamp,
  r.source,
  r.phase,
	g.next_sequence_count=g.last_sequence_count,
	h.replay_id,
	c.id is not null
from vmu_packet_gap g
  join vmu_record r on g.vmu_record_id=r.id
	join gap_replay_list h using (hrd_packet_gap_id)
	left outer join completed_replays c on c.id=h.replay_id
	where g.timestamp >= (select date from days_back);
