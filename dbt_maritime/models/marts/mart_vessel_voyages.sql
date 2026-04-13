with positions as (
    select * from {{ ref('stg_ais_positions') }}
),

weather as (
    select * from {{ ref('stg_ocean_weather') }}
),

-- Find the nearest weather observation for each position
positions_with_weather as (
    select
        p.mmsi,
        p.imo_number,
        p.vessel_name,
        p.ship_type_code,
        p.latitude,
        p.longitude,
        p.speed_knots,
        p.course,
        p.heading,
        p.destination,
        p.navigation_status,
        p.received_at,
        date_trunc('day', p.received_at) as voyage_date,

        -- Find closest weather point (simple distance approximation)
        w.wave_height_m,
        w.wind_speed_ms,
        w.wind_speed_knots,
        w.sea_state,
        w.water_temperature_c,

        row_number() over (
            partition by p.mmsi, p.received_at
            order by
                abs(p.latitude - w.latitude) + abs(p.longitude - w.longitude),
                abs(datediff('hour', p.received_at, w.forecast_time))
        ) as weather_rank

    from positions p
    left join weather w
        on abs(p.latitude - w.latitude) < 2
        and abs(p.longitude - w.longitude) < 2
        and abs(datediff('hour', p.received_at, w.forecast_time)) < 6
)

select
    mmsi,
    imo_number,
    vessel_name,
    ship_type_code,
    latitude,
    longitude,
    speed_knots,
    course,
    heading,
    destination,
    navigation_status,
    received_at,
    voyage_date,
    wave_height_m,
    wind_speed_ms,
    wind_speed_knots,
    sea_state,
    water_temperature_c
from positions_with_weather
where weather_rank = 1 or weather_rank is null
