create or replace view record_infos(phase, total) as
  with records(record_id, count) as (
    select
      vmu_record_id,
      count(vmu_record_id)
    from vmu_packet_gap
    group by vmu_record_id
  )
  select
    r.phase,
    sum(g.count)
  from vmu_record r
  join records g on r.id=g.record_id
  where r.phase is not null
  group by r.phase
