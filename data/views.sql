drop view if exists apidaysback;
drop view if exists days_back;
drop view if exists completed_workflows;
drop view if exists pending_workflows;
drop view if exists pending_workflow;
drop view if exists cancelled_workflows;
drop view if exists cancelled_workflow;
drop view if exists exited_workflows;
drop view if exists running_workflows;
drop view if exists latest_status;
drop view if exists recent_status;
drop view if exists completed_replays;
drop view if exists channel_infos;
drop view if exists hrd_gap_list;
drop view if exists hrd_status_list;
drop view if exists items_count;
drop view if exists jobs_status;
drop view if exists records_count;
drop view if exists record_infos;
drop view if exists source_infos;
drop view if exists corrupted_hrd_list;
drop view if exists missing_hrd_list;
drop view if exists replay_job_list;
drop view if exists automatic_replay_list;
drop view if exists replay_list;
drop view if exists vmu_gap_list;
drop view if exists max_latest_status;
drop view if exists pending_duration;

create view apidaysback as
	select ifnull((select value from variable where name='api_days_back' limit 1), 15) as 'day';

create view days_back as
	select date(current_date, (select -day || ' days' from apidaysback)) as 'date';

create view completed_workflows as
	select workflow as 'wf' from replay_status order by workflow desc limit 4;

create view pending_workflow as
	select min(workflow) as 'wf' from replay_status;

create view cancelled_workflow as
	select max(workflow) as 'wf' from replay_status;

create view exited_workflows as
	select workflow as 'wf' from replay_status order by workflow desc limit 4 offset 1;

create view running_workflows as
	select
		workflow as 'wf'
	from replay_status
	where workflow <> (select wf from pending_workflow)
		or workflow not in (select wf from completed_workflows);

create view latest_status as
  select
    replay_id as 'replay',
    date(timestamp) as 'date',
    max(replay_status_id) as 'status'
  from replay_job
  where timestamp >= (select date from days_back)
  group by date, replay_id
  order by replay_id;

create view recent_status as
	select
		replay_id as 'replay',
		max(timestamp) as 'date',
		max(replay_status_id) as 'status'
	from replay_job
	where timestamp >= (select date from days_back)
	group by replay_id
	order by replay_id;

create view completed_replays as
	select
		replay_id as 'id'
	from replay_job
	where replay_status_id in (
		select
			id
		from replay_status
		where workflow in (select wf from completed_workflows)
	);

create view channel_infos as
  select
    chanel as 'channel',
    count(chanel) as 'total'
  from hrd_packet_gap
  where timestamp >= (select date from days_back)
  group by chanel;

create view hrd_gap_list as
select
  h.id as 'id',
  h.timestamp as 'timestamp',
  h.chanel as 'channel',
  h.last_sequence_count as 'last_sequence_count',
  h.last_timestamp as 'last_timestamp',
  h.next_sequence_count as 'next_sequence_count',
  h.next_timestamp as 'next_timestamp',
  h.next_sequence_count=h.last_sequence_count as 'corrupted',
	r.id is not null as 'completed',
  i.replay_id as 'replay'
from hrd_packet_gap h
  join gap_replay_list i on i.hrd_packet_gap_id=h.id
  left outer join completed_replays r on r.id=i.replay_id
  where h.timestamp >= (select date from days_back);

create view hrd_status_list as
  select
    'CORRUPTED' as 'label',
    date(timestamp) as 'timestamp',
    channel as 'channel',
    count(id) as 'count'
  from hrd_gap_list
  where corrupted
  group by date(timestamp), channel
  union all
  select
    'MISSING',
    date(timestamp) as date,
    channel,
    sum(next_sequence_count-last_sequence_count)
  from hrd_gap_list
  where not corrupted
  group by date(timestamp), channel;

create view items_count as
  select
    'REPLAY' as 'label',
    'ALL' as 'origin',
    date(timestamp) as 'date',
    count(id) as 'count',
    0 as 'missing',
    sum(strftime('%s', enddate) - strftime('%s', startdate)) as 'duration'
  from replay
  where enddate > startdate
    and replay.timestamp >= (select date from days_back)
  group by date(timestamp)
  union all
  select
    'HRD' as label,
    chanel as origin,
    date(timestamp) as 'date',
    count(id) as total,
    sum(next_sequence_count-last_sequence_count),
    sum(strftime('%s', next_timestamp) - strftime('%s', last_timestamp)) as duration
  from hrd_packet_gap
  where next_timestamp > last_timestamp
    and hrd_packet_gap.timestamp >= (select date from days_back)
  group by date(timestamp), chanel
  union all
  select
    'VMU' as label,
    r.source as origin,
    date(g.timestamp) as 'date',
    count(g.id) as total,
    sum(next_sequence_count-last_sequence_count),
    sum(strftime('%s', next_timestamp) - strftime('%s', last_timestamp)) as duration
  from vmu_packet_gap as g
    inner join vmu_record as r on g.vmu_record_id=r.id
  where next_timestamp > last_timestamp
    and g.timestamp >= (select date from days_back)
  group by date(timestamp), r.source;

create view jobs_status as
select
	'PENDING' as 'label',
	date as 'timestamp',
	count(replay) as 'count'
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

create view records_count as
	select
		vmu_record_id as 'id',
		count(vmu_record_id) as 'total'
	from vmu_packet_gap
	where timestamp >= (select date from days_back)
	group by vmu_record_id;

create view source_infos as
  select
    r.source as 'source',
    sum(g.total) as 'total'
  from vmu_record r
  join records_count g on r.id=g.id
  where r.source is not null
  group by r.source;

create view record_infos as
  select
    r.phase as 'phase',
    sum(g.total) as 'total'
  from vmu_record r
  join records_count g on r.id=g.id
  where r.phase is not null
  group by r.phase;

create view corrupted_hrd_list as
	select
		replay as 'id',
		count(id) as 'total'
	from hrd_gap_list
	where corrupted and timestamp >= (select date from days_back)
	group by replay;

create view missing_hrd_list as
	select
		replay as 'id',
		sum(next_sequence_count-last_sequence_count) as 'total'
	from hrd_gap_list
	where timestamp >= (select date from days_back)
	group by replay;

create view replay_job_list as
	select
		j.replay_id as 'replay',
		j.text as 'text',
		j.replay_status_id as 'status',
		j.timestamp as 'timestamp'
	from replay_job j
	join recent_status s on j.replay_id=s.replay and j.replay_status_id=s.status
	where j.timestamp >= (select date from days_back);

create view automatic_replay_list as
	select
		replay as 'replay',
		count(replay) as 'total'
	from hrd_gap_list
	where timestamp >= (select date from days_back)
	group by replay;

create view replay_list as
	select
		r.id as 'id',
		j.timestamp as 'timestamp',
		r.startdate as 'startdate',
		r.enddate as 'enddate',
		coalesce(r.priority, -1) as 'priority',
		coalesce(j.text, '') as 'comment',
		s.name as 'status',
		g.replay is not null as 'automatic',
		-- replay_status_id not in (select * from cancellable) as cancellable,
		s.workflow not in (select wf from completed_workflows) as 'cancellable',
	  0 as 'corrupted',
		0 as 'missing'
	from replay as r
		inner join replay_job_list as j on r.id = j.replay
		inner join replay_status as s on s.id = j.status
		left outer join automatic_replay_list as g on r.id=g.replay
	  -- left outer join corrupted_hrd_list as c on c.id=r.id
		-- left outer join missing_hrd_list as m on m.id=r.id
		where r.timestamp >= (select date from days_back);

create view vmu_gap_list as
select
	g.id as 'id',
  g.timestamp as 'timestamp',
  g.last_sequence_count as 'last_sequence_count',
  g.last_timestamp as 'last_timestamp',
  g.next_sequence_count as 'next_sequence_count',
  g.next_timestamp as 'next_timestamp',
  r.source as 'source',
  r.phase as 'phase',
	g.next_sequence_count=g.last_sequence_count as 'corrupted',
	h.replay_id as 'replay',
	c.id is not null as 'completed'
from vmu_packet_gap g
  join vmu_record r on g.vmu_record_id=r.id
	join gap_replay_list h using (hrd_packet_gap_id)
	left outer join completed_replays c on c.id=h.replay_id
	where g.timestamp >= (select date from days_back);


create view max_latest_status as
	select
		replay as 'replay',
		max(date) as 'date',
		max(status) as 'status'
	from latest_status
	group by replay;


create view pending_duration as
	select
		coalesce(sum(strftime('%s', r.enddate)-strftime('%s', r.startdate)), 0) as 'duration'
	from max_latest_status s
		join replay r on s.replay=r.id
        join replay_status rs on rs.id=s.status
	where rs.workflow not in (select wf from completed_workflows);
