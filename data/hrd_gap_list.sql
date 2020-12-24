create or replace view hrd_gap_list(id, timestamp, channel, last_sequence_count, last_timestamp, next_sequence_count, next_timestamp, corrupted, replay, completed) as
select
h.id,
h.timestamp,
h.chanel,
h.last_sequence_count,
h.last_timestamp,
h.next_sequence_count,
h.next_timestamp,
h.next_sequence_count=h.last_sequence_count,
i.replay_id,
r.id is not null
from hrd_packet_gap h
  join gap_replay_list i on i.hrd_packet_gap_id=h.id
  left outer join completed_replays r on r.id=i.replay_id
