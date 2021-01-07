create or replace view replay_list(id, timestamp, startdate, enddate, priority, comment, status, automatic, cancellable, corrupted, missing) as
	with
		cancellable(id) as (select id from replay_status order by workflow desc limit 4),
	  corrupted(id, count) as (select replay, count(id) from hrd_gap_list group by replay),
		missing(id, count) as (select replay, sum(next_sequence_count-last_sequence_count) from hrd_gap_list group by replay)
	select
		r.id,
		r.timestamp,
		r.startdate,
		r.enddate,
		coalesce(r.priority, -1) as priority,
		coalesce(j.text, '') as comment,
		s.name,
		g.replay_id is not null as automatic,
		replay_status_id not in (select * from cancellable) as cancellable,
	  coalesce(c.count, 0) as corrupted,
		coalesce(m.count, 0) as missing
	from replay as r
		inner join (
	  	select
				j.replay_id,
				j.text,
				m.replay_status_id
			from replay_job as j
	    inner join (
				select
					replay_id,
					max(replay_status_id) as replay_status_id
				from replay_job
				where timestamp >= (select date from days_back)
				group by replay_id
			) as m using (replay_id, replay_status_id)
			where timestamp >= (select date from days_back)
		) as j on r.id = j.replay_id
		inner join replay_status as s on s.id = j.replay_status_id
		left outer join (select distinct replay_id from gap_replay_list) as g on r.id = g.replay_id
	  left outer join corrupted as c on c.id=r.id
		left outer join missing as m on m.id=r.id
		where r.timestamp >= (select date from days_back);
