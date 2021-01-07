create or replace view days_back(date) as
with
	days(d) as (select ifnull((select value from variable where name='api_days_back' limit 1), 15))
	select date_sub(current_date(), interval (select d from days) DAY);

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
with
	workflows(wf) as (select workflow from replay_status order by workflow desc limit 4),
	completed(id) as (select id from replay_status where workflow in (select wf from workflows))
select replay_id from replay_job where replay_status_id in (select id from completed);
