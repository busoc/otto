create or replace view channel_infos(channel, total) as
  select
    chanel,
    count(chanel)
  from hrd_packet_gap
  where timestamp >= (select date from days_back)
  group by chanel;
