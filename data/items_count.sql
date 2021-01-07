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
