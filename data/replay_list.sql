create or replace view replay_list(id, timestamp, startdate, enddate, priority, comment, status, automatic, cancellable) as
WITH
	cancellable(id) AS (SELECT id FROM replay_status ORDER BY workflow DESC LIMIT 4)
SELECT
	r.id, r.timestamp, r.startdate, r.enddate,
	COALESCE(r.priority, -1) as priority, COALESCE(j.text, '') as comment, s.name, g.replay_id IS NOT NULL as automatic,
  replay_status_id NOT IN (SELECT * FROM cancellable) as cancellable
FROM replay AS r
INNER JOIN (
	SELECT j.replay_id, j.text, m.replay_status_id FROM replay_job AS j
    INNER JOIN (SELECT replay_id, MAX(replay_status_id) AS replay_status_id FROM replay_job GROUP BY replay_id) AS m USING (replay_id, replay_status_id)
) AS j ON r.id = j.replay_id INNER JOIN replay_status AS s ON s.id = j.replay_status_id
LEFT OUTER JOIN (SELECT DISTINCT replay_id FROM gap_replay_list) AS g ON r.id = g.replay_id
