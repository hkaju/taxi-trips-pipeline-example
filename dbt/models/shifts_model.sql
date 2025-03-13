{{ config(materialized = 'table') }}

with shifts as (
	select
        taxi_id,
        unnest(range_agg(tsrange(trip_start, trip_end + interval '1 hour'))) as shift_duration
    from pipeline.trips
    group by
        taxi_id
),
final as (
    select
        md5(random()::text) as shift_id,
        date_trunc('week', lower(shift_duration)) as shift_week,
        s.taxi_id,
        s.shift_duration,
        count(t) as trip_count,
        coalesce(sum(t.trip_total), 0) as shift_total,
        coalesce(sum(t.trip_miles), 0) as shift_miles,
        coalesce(sum(t.trip_seconds), 0) as shift_seconds,
        max(t.created_at) as updated_at
    from shifts s
    join pipeline.trips t on
        t.taxi_id = s.taxi_id
        and t.trip_start >= lower(s.shift_duration) and t.trip_end <= upper(s.shift_duration)
    group by
        s.shift_duration, s.taxi_id
)
select * from final
