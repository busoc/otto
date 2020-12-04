create or replace view items_count(label, date, count) as
select 'REPLAY' as label, date(timestamp) as date, count(id) as total from replay group by date
union all
select 'HRD' as label, date(timestamp) as date, count(id) as total from hrd_packet_gap group by date
union all
select 'VMU' as label, date(timestamp) as date, count(id) as total from vmu_packet_gap group by date
