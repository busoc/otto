create or replace view items_count(label, origin, date, count, duration) as
  select 'REPLAY' as label, 'ALL' as origin, date(timestamp) as date, count(id) as total, sum(unix_timestamp(enddate) - unix_timestamp(startdate)) as duration
  from replay group by date
  union all
  select 'HRD' as label, chanel as origin, date(timestamp) as date, count(id) as total, sum(unix_timestamp(next_timestamp) - unix_timestamp(last_timestamp)) as duration
  from hrd_packet_gap group by chanel, date
  union all
  select 'VMU' as label, r.source as origin, date(g.timestamp) as date, count(g.id) as total, sum(unix_timestamp(next_timestamp) - unix_timestamp(last_timestamp)) as duration
  from vmu_packet_gap as g join vmu_record as r on g.vmu_record_id=r.id group by r.source, date
