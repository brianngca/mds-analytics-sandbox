--CREATE A MASTER TABLE FOR QA
-- drop table vehicle_state_uat1;
create table vehicle_state_uat1
as
with event_pairs as (
	SELECT
		e.id,
		e.provider_id,
		case 
	        when e.provider_id = '6ddcc0ad-1d66-4046-bba4-d1d96bb8ca4f' then 'Razor'
	        when e.provider_id = '2e4cb206-b475-4a9d-80fb-0880c9a033e1' then 'HOPR'
	        when e.provider_id = 'd56d2df6-fa92-43de-ab61-92c3a84acd7d' then 'WIND'
	        when e.provider_id = '264aad41-b47c-415d-8585-0208d436516e' then 'Tier'
	        when e.provider_id = 'bf95148b-d1d1-464e-a140-6d2563ac43d4' then 'Cloud'    
	    	else 'unknown'
		end as provider_name,
		e.device_id,
		e.trip_id,
		e.event_type AS this_event_type,
		to_timestamp(e.timestamp/1000) AS this_date,
		to_timestamp(e.recorded/1000) AS this_recorded,
		lag(e.event_type, 1) OVER (PARTITION BY e.device_id ORDER BY to_timestamp(e.timestamp/1000)) prev_event_type,
		lag(to_timestamp(e.timestamp/1000), 1) OVER (PARTITION BY e.device_id ORDER BY to_timestamp(e.timestamp/1000)) prev_date,
		lead(e.event_type, 1) OVER (PARTITION BY e.device_id ORDER BY to_timestamp(e.timestamp/1000)) next_event_type,
		lead(to_timestamp(e.timestamp/1000), 1) OVER (PARTITION BY e.device_id ORDER BY to_timestamp(e.timestamp/1000)) next_date
	FROM
		events_uat1 e
	ORDER BY to_timestamp(e.timestamp/1000) ASC
)
SELECT
    e.id,
    e.provider_id,
    e.provider_name,
    e.device_id,
    e.trip_id,
    e.prev_event_type,
    e.prev_date,
    e.this_event_type,
    e.this_date,
    e.this_recorded,
    e.this_recorded - e.this_date as recorded_delay,
    e.next_event_type,
    e.next_date,
	CASE WHEN e.this_event_type IN('trip_end', 'provider_drop_off', 'service_start', 'cancel_reservation')
		THEN 'available'
	WHEN e.this_event_type = 'service_end'
		THEN 'unavailable'
	WHEN e.this_event_type = 'reserve'
		THEN 'reserved'
	WHEN e.this_event_type IN('trip_enter', 'trip_start')
		THEN 'trip'
	WHEN e.this_event_type IN('provider_pick_up', 'register')
		THEN 'removed'
	WHEN e.this_event_type = 'trip_leave'
		THEN 'elsewhere'
	WHEN e.this_event_type = 'deregister'
		THEN 'inactive'
	ELSE 'error_state'
	END AS state,
	CASE WHEN (e.this_event_type IN('trip_end', 'provider_drop_off', 'service_start', 'cancel_reservation')
		AND e.next_event_type IN ('deregister', 'service_end', 'provider_pick_up', 'trip_start', 'reserve'))
		OR (e.this_event_type = 'service_end' AND e.next_event_type in ('deregister', 'service_start', 'provider_pick_up'))
		OR (e.this_event_type = 'reserve' AND e.next_event_type in ('cancel_reservation', 'trip_start'))
		OR (e.this_event_type IN('trip_enter', 'trip_start') AND e.next_event_type in ('trip_end', 'trip_leave'))
		OR (e.this_event_type IN('provider_pick_up', 'register') AND e.next_event_type in ('deregister', 'provider_drop_off', 'trip_enter'))
		OR (e.this_event_type = 'trip_leave' AND e.next_event_type IN ('deregister', 'provider_pick_up', 'trip_enter', 'provider_drop_off'))
		OR (e.this_event_type = 'deregister' AND e.next_event_type in ('register'))
		THEN 1
	ELSE 0
	END AS is_state_valid,
	CASE WHEN e.next_date IS NULL THEN
		tstzrange(e.this_date, e.this_date)
	WHEN e.this_date IS NULL THEN
		tstzrange(e.next_date, e.next_date)
	ELSE
		tstzrange(e.this_date, e.next_date)
	END AS state_ts_range
FROM event_pairs e;


--EVENTS.[EVENT_TYPE]: 15 MINUTES
WITH measure_dates as (
  --Select every date between range
	select generate_series('2020-01-01 00:00:00-00'::timestamptz, '2020-12-31 00:00:00-00', '15 minutes') as timebin_lower
), qa as (
	select 
		m.timebin_lower as qa_time_bin_start,
		concat('events.', s.this_event_type) as qa_name,
		count(s.*) as qa_count
	from 
		vehicle_state s,
		measure_dates m
	where 
		s.this_date between m.timebin_lower and (m.timebin_lower + interval '15 minutes')
		and s.this_recorded between m.timebin_lower and (m.timebin_lower + interval '15 minutes')
	-- and s.is_state_valid = 1
	group by 1, 2
	order by 1 desc, 2 asc
), metric as (
	select 
		to_timestamp(time_bin_start/1000) as time_bin_start, 
		time_bin_size,
		name,
		count as metric_count
	from metrics 
	where time_bin_size = (1000 * 60 * 15)
	order by 1 desc, name asc
) 
select distinct
	m.*,
	q.qa_count,
	case when metric_count = qa_count then 1
		else 0 end as is_match
from 
	qa q
join metric m on (q.qa_time_bin_start = m.time_bin_start and q.qa_name = m.name)
order by 1 desc;


--EVENTS.[EVENT_TYPE]: 1 HOUR
WITH measure_dates as (
  --Select every date between range
	select generate_series('2020-01-01 00:00:00-00'::timestamptz, '2020-12-31 00:00:00-00', '1 hour') as timebin_lower
), qa as (
	select 
		m.timebin_lower as qa_time_bin_start,
		concat('events.', s.this_event_type) as qa_name,
		count(s.*) as qa_count
	from 
		vehicle_state s,
		measure_dates m
	where 
		s.this_date between m.timebin_lower and (m.timebin_lower + interval '1 hour')
		and s.this_recorded between m.timebin_lower and (m.timebin_lower + interval '1 hour')
	-- and s.is_state_valid = 1
	group by 1, 2
	order by 1 desc, 2 asc
), metric as (
	select 
		to_timestamp(time_bin_start/1000) as time_bin_start, 
		time_bin_size,
		name,
		count
	from metrics 
	where time_bin_size = 3600000
	order by 1 desc, name asc
) 
select distinct
	m.*,
	q.qa_count,
	case 
		when count = qa_count then 1
		else 0 end as is_match
from 
	qa q
join metric m on (q.qa_time_bin_start = m.time_bin_start and q.qa_name = m.name)
where count != qa_count
order by 1 desc;



--EVENTS.[EVENT_TYPE]: 1 DAY
WITH measure_dates as (
  --Select every date between range
	select generate_series('2020-01-01 00:00:00-00'::timestamptz, '2020-12-30 00:00:00-00', '1 day') as timebin_lower
), qa as (
	select 
		m.timebin_lower as qa_time_bin_start,
		concat('events.',s.this_event_type) as qa_name,
		count(s.*) as qa_count
	from 
		vehicle_state s,
		measure_dates m
	where 
		s.this_date between m.timebin_lower and (m.timebin_lower + interval '1 day')
		and s.this_recorded between m.timebin_lower and (m.timebin_lower + interval '1 day')
	-- and s.is_state_valid = 1
	group by 1, 2
	order by 1 desc, 2 asc
), metric as (
	select 
		to_timestamp(time_bin_start/1000) as time_bin_start, 
		time_bin_size,
		name,
		count as metric_count
	from metrics 
	where time_bin_size = 86400000
	order by 1 desc, name asc
) 
select distinct
	m.*,
	q.qa_count,
	case when metric_count = qa_count then 1
		else 0 end as is_match
from 
	qa q
join metric m on (q.qa_time_bin_start = m.time_bin_start and q.qa_name = m.name)
where metric_count != qa_count
order by 1 desc;


--VEHICLES.[STATE].AVG: 15 MINS
WITH measure_dates as (
  --Select every date between range
	select generate_series('2020-01-01 00:00:00-00'::timestamptz, '2020-12-30 00:00:00-00', '15 minutes') as timebin_lower
), state_intersect AS (
	SELECT
		m.timebin_lower as timebin,
		tstzrange(m.timebin_lower::timestamptz, (m.timebin_lower + interval '15 minutes')::timestamptz) * s.state_ts_range AS time_intersect,
		s.state,
		s.device_id
	FROM 
	    vehicle_state_uat1 s,
	    measure_dates m
	WHERE
		tstzrange(m.timebin_lower::timestamptz, (m.timebin_lower + interval '15 minutes')::timestamptz) && s.state_ts_range
), interval_summary AS (
	SELECT
		timebin,
		state,
		SUM(UPPER(time_intersect) - LOWER(time_intersect)) AS time_sum
	FROM 
	    state_intersect
    GROUP BY 1, 2
), extract_sum as (	
	SELECT
		timebin,
		state,
		(EXTRACT(DAY FROM time_sum) * (24 * 3600) 
		+ EXTRACT(HOUR FROM time_sum) * 3600 
		+ EXTRACT(MINUTE FROM time_sum) * 60 
		+ EXTRACT(SECOND FROM time_sum)) AS vehicle_sum
	from interval_summary
)
SELECT
	s.timebin,
	s.state,
	round(s.vehicle_sum / 900) as avg_deployed_vehicles
FROM extract_sum s;



--VEHICLES.[STATE].AVG: 1 HOUR
WITH measure_dates as (
	select generate_series('2020-01-01 00:00:00-00'::timestamptz, '2020-12-30 00:00:00-00', '1 hour') as timebin_lower
), state_intersect AS (
	SELECT
		m.timebin_lower as timebin,
		tstzrange(m.timebin_lower::timestamptz, (m.timebin_lower + interval '1 hour')::timestamptz) * s.state_ts_range AS time_intersect,
		s.state,
		s.device_id
	FROM 
	    vehicle_state_uat1 s,
	    measure_dates m
	WHERE
		tstzrange(m.timebin_lower::timestamptz, (m.timebin_lower + interval '1 hour')::timestamptz) && s.state_ts_range
), interval_summary AS (
	SELECT
		timebin,
		state,
		SUM(UPPER(time_intersect) - LOWER(time_intersect)) AS time_sum
	FROM 
	    state_intersect
    GROUP BY 1, 2
), extract_sum as (	
	SELECT
		timebin,
		state,
		(EXTRACT(DAY FROM time_sum) * (24 * 3600) 
		+ EXTRACT(HOUR FROM time_sum) * 3600 
		+ EXTRACT(MINUTE FROM time_sum) * 60 
		+ EXTRACT(SECOND FROM time_sum)) AS vehicle_sum
	from interval_summary
)
SELECT
	s.timebin,
	s.state,
	round(s.vehicle_sum / 3600) as avg_deployed_vehicles
FROM extract_sum s;



--VEHICLES.[STATE].MIN/MAX/AVG (SNAPSHOT EVERY 1 MIN): 15 MINS, 1 HOUR, 1 DAY
--10 SECS FOR 1 DAY DATA, 1 MIN SNAPSHOT
WITH snapshot_date as (
	select generate_series('2020-05-12 00:00:00-00'::timestamptz, '2020-05-13 00:00:00-00', '1 minute') as timebin_lower
), snapshot AS (
	SELECT
		m.timebin_lower as timebin,
		s.state,
		count(distinct s.device_id) filter (where tstzrange(m.timebin_lower::timestamptz, (m.timebin_lower + interval '1 minute')::timestamptz) && s.state_ts_range)
	FROM 
	    vehicle_state_uat1 s,
	    snapshot_date m
    where tstzrange(m.timebin_lower::timestamptz, (m.timebin_lower + interval '1 minute')::timestamptz) && s.state_ts_range
    group by 1, 2
), state_date as (
	select 
		generate_series('2020-05-12 00:00:00-00'::timestamptz, '2020-05-13 00:00:00-00', '15 minutes') as timebin_15_min,
		generate_series('2020-05-12 00:00:00-00'::timestamptz, '2020-05-13 00:00:00-00', '1 hour') as timebin_1_hr,
		generate_series('2020-05-12 00:00:00-00'::timestamptz, '2020-05-13 00:00:00-00', '1 day') as timebin_1_day
)
select 
	d.timebin_15_min as time_bin_start,
	900 as time_bin_size,	
	concat('vehicles.',s.state) as name,
	max(s.count),
	min(s.count),
	round(avg(s.count)) as avg
from 
	state_date d,
	snapshot s
where s.timebin between d.timebin_15_min and (d.timebin_15_min + interval '15 minutes')
group by 1, 2, 3

union all 
select 
	d.timebin_1_hr as time_bin_start,
	3600 as time_bin_size,
	concat('vehicles.',s.state) as name,
	max(s.count),
	min(s.count),
	round(avg(s.count)) as avg
from 
	state_date d,
	snapshot s
where s.timebin between d.timebin_1_hr and (d.timebin_1_hr + interval '1 hour')
group by 1, 2, 3

union all 
select 
	d.timebin_1_day as time_bin_start,
	86400 as time_bin_size,
	concat('vehicles.',s.state) as name,
	max(s.count),
	min(s.count),
	round(avg(s.count)) as avg
from 
	state_date d,
	snapshot s
where s.timebin between d.timebin_1_day and (d.timebin_1_day + interval '1 day')
group by 1, 2, 3;



--ANOTHER WAY TO CALCULATE VEHICLES.[STATE].AVG: 1 DAY
WITH measure_dates as (
	select generate_series('2020-01-01 00:00:00-00'::timestamptz, '2020-12-30 00:00:00-00', '1 day') as timebin_lower
), state_intersect AS (
	SELECT
		m.timebin_lower as timebin,
		tstzrange(m.timebin_lower::timestamptz, (m.timebin_lower + interval '1 day')::timestamptz) * s.state_ts_range AS time_intersect,
		s.state,
		s.device_id
	FROM 
	    vehicle_state_uat1 s,
	    measure_dates m
	WHERE
		tstzrange(m.timebin_lower::timestamptz, (m.timebin_lower + interval '1 day')::timestamptz) && s.state_ts_range
), interval_summary AS (
	SELECT
		timebin,
		state,
		SUM(UPPER(time_intersect) - LOWER(time_intersect)) AS time_sum
	FROM 
	    state_intersect
    GROUP BY 1, 2
), extract_sum as (	
	SELECT
		timebin,
		state,
		(EXTRACT(DAY FROM time_sum) * (24 * 3600) 
		+ EXTRACT(HOUR FROM time_sum) * 3600 
		+ EXTRACT(MINUTE FROM time_sum) * 60 
		+ EXTRACT(SECOND FROM time_sum)) AS vehicle_sum
	from interval_summary
)
SELECT
	s.timebin,
	s.state,
	round(s.vehicle_sum / 86400) as avg_deployed_vehicles
FROM extract_sum s;



--TRIPS: 15 MINUTES
WITH measure_dates as (
  --Select every date between range
	select generate_series('2020-04-20 00:00:00-00'::timestamptz, '2020-12-31 00:00:00-00', '15 minutes') as timebin_lower
)
select 
	m.timebin_lower as qa_time_bin_start,
	'trips' as name,
	count(distinct s.trip_id) as qa_count
from 
	vehicle_state_uat1 s,
	measure_dates m
where 
	s.this_date between m.timebin_lower and (m.timebin_lower + interval '15 minutes')
	and s.this_recorded between m.timebin_lower and (m.timebin_lower + interval '15 minutes')
	and s.this_event_type in ('trip_end', 'trip_leave')
-- and s.is_state_valid = 1
group by 1
order by 1 desc;



--TRIPS.DURATION: 1 DAY
WITH measure_dates as (
	select generate_series('2020-01-01 00:00:00-00'::timestamptz, '2020-12-30 00:00:00-00', '1 day') as timebin_lower
), state_intersect AS (
	SELECT
		m.timebin_lower as timebin,
		tstzrange(m.timebin_lower::timestamptz, (m.timebin_lower + interval '1 day')::timestamptz) * s.state_ts_range AS time_intersect,
		s.state,
		s.device_id
	FROM 
	    vehicle_state_uat1 s,
	    measure_dates m
	WHERE
		tstzrange(m.timebin_lower::timestamptz, (m.timebin_lower + interval '1 day')::timestamptz) && s.state_ts_range
), interval_summary AS (
	SELECT
		timebin,
		state,
		SUM(UPPER(time_intersect) - LOWER(time_intersect)) AS time_sum
	FROM 
	    state_intersect
    GROUP BY 1, 2
)
SELECT
	timebin,
	state,
	(EXTRACT(DAY FROM time_sum) * (24 * 3600) 
	+ EXTRACT(HOUR FROM time_sum) * 3600 
	+ EXTRACT(MINUTE FROM time_sum) * 60 
	+ EXTRACT(SECOND FROM time_sum)) AS duration_secs
from interval_summary
where state = 'trip';


--VEHICLES.TELEMETRY: 15 MINUTES
WITH measure_dates as (
	select generate_series('2020-04-20 00:00:00-00'::timestamptz, '2020-12-31 00:00:00-00', '15 minutes') as timebin
)
select 
	m.timebin as qa_time_bin_start,
	'900' as time_bin_size,
	'vehicles.telemetry' as name,
	count(s.*) as qa_count
from 
	telemetry s,
	measure_dates m
where 
	to_timestamp(s.recorded/1000) between m.timebin and (m.timebin + interval '15 minutes')
group by 1, 2, 3;



--VEHICLES.TELEMETRY.LATENCY_SLA: 15 MINUTES
WITH measure_dates as (
	select generate_series('2020-05-13 00:00:00-00'::timestamptz, '2020-05-14 00:00:00-00', '15 minutes') as timebin
)
select 
	m.timebin as qa_time_bin_start,
	'900' as time_bin_size,
	'vehicles.telemetry.latency_sla' as name,
	count(s.*) filter (
		where (to_timestamp(recorded/1000) - to_timestamp(timestamp/1000)) > interval '24 hours') as qa_count
from 
	telemetry s,
	measure_dates m
where 
	to_timestamp(s.recorded/1000) between m.timebin and (m.timebin + interval '15 minutes')
group by 1, 2, 3;



--EVENTS.[EVENT_TYPE].LATENCY_SLA: 15 MINUTES
WITH measure_dates as (
	select generate_series('2020-05-13 00:00:00-00'::timestamptz, '2020-05-14 00:00:00-00', '15 minutes') as timebin
)
select 
	m.timebin as qa_time_bin_start,
	'900' as time_bin_size,
	concat('events.',s.event_type,'.latency_sla') as name,
	count(s.*) filter (
		where (to_timestamp(s.recorded/1000) - to_timestamp(s.timestamp/1000)) > interval '24 hours') as qa_count
from 
	events_uat1 s,
	measure_dates m
where 
	to_timestamp(s.recorded/1000) between m.timebin and (m.timebin + interval '15 minutes')
	and s.event_type in ('trip_start', 'trip_end', 'tri_enter', 'trip_leave')
group by 1, 2, 3;


select * from telemetry limit 1;


--to debug events
select 
	to_timestamp(recorded/1000) as recorded_ts,
	to_timestamp("timestamp"/1000) as event_ts,
	event_type,
	* 
from events
order by 2 desc;
limit 1;
