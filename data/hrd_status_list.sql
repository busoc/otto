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
  group by date, channel
