create or replace view vmu_gap_list(id, timestamp, last_sequence_count, last_timestamp, next_sequence_count, next_timestamp, source, phase, corrupted) as
select
	g.id,
    g.timestamp,
    g.last_sequence_count,
    g.last_timestamp,
    g.next_sequence_count,
    g.next_timestamp,
    r.source,
    r.phase,
		g.next_sequence_count=g.last_sequence_count
from vmu_packet_gap g
  join vmu_record r on g.vmu_record_id=r.id
