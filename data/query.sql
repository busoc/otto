select * from variable;

select
  r.id, r.timestamp, r.startdate, r.enddate, coalesce(r.priority, -1) as priority, coalesce(j.text, "") as comment, s.name as status, !isnull(g.replay_id) as automatic
from
  replay as r
  join replay_job as j on r.id=j.replay_id
  join replay_status as s on j.replay_status_id=s.id
  left outer join gap_replay_list g on r.id=g.replay_id

select id, timestamp, last_timestamp, last_sequence_count, next_timestamp, next_sequence_count, chanel from hrd_packet_gap

select
  g.id, g.timestamp, g.last_timestamp, g.last_sequence_count, g.next_timestamp, g.next_sequence_count, r.source, r.phase
from
  vmu_packet_gap as g
  join vmu_record as r on g.vmu_record_id=r.id
