{{config(
    materialized = 'incremental',
    incremental_strategy="merge",
    unique_key="shift_id"
)}}

with shifts as (
	select
        taxi_id,
        unnest(range_agg(tsrange(trip_duration))) as shift_duration
    from pipeline.trips
    where
        1=1
        {% if is_incremental() %}
        and created_at > coalesce((select max(updated_at) from {{ this }}), (select to_timestamp(0)))
        {% endif %}
    group by
        taxi_id
),
new as (
    select
        md5(random()::text) as shift_id,
        s.taxi_id,
        s.shift_duration,
        count(t) as trip_count,
        sum(t.trip_seconds) as shift_seconds,
        sum(t.trip_miles) as shift_miles,
        sum(t.fare) as shift_fare,
        sum(t.tips) as shift_tips,
        sum(t.tolls) as shift_tolls,
        sum(t.extras) as shift_extras,
        sum(t.trip_total) as shift_total,
        max(t.created_at) as updated_at
    from shifts s
    join pipeline.trips t on
        t.taxi_id = s.taxi_id
        and t.trip_duration && s.shift_duration
    group by
        s.shift_duration, s.taxi_id
)

{% if is_incremental() %}
, reaggregate as (
    select
        coalesce(existing.shift_id, new.shift_id) as shift_id,
        new.taxi_id,
        new.shift_duration + coalesce(existing.shift_duration, new.shift_duration) as shift_duration,
        coalesce(existing.trip_count, 0) + new.trip_count as trip_count,
        coalesce(existing.shift_seconds, 0) + new.shift_seconds as shift_seconds,
		coalesce(existing.shift_miles, 0) + new.shift_miles as shift_miles,
        coalesce(existing.shift_fare, 0) + new.shift_fare as shift_fare,
        coalesce(existing.shift_tips, 0) + new.shift_tips as shift_tips,
        coalesce(existing.shift_tolls, 0) + new.shift_tolls as shift_tolls,
        coalesce(existing.shift_extras, 0) + new.shift_extras as shift_extras,
        coalesce(existing.shift_total, 0) + new.shift_total as shift_total,
        new.updated_at
    from new
        left join {{ this }} as existing
            on new.taxi_id = existing.taxi_id
            and new.shift_duration && existing.shift_duration
)
select * from reaggregate
{% else %}
select * from new
{% endif %}
