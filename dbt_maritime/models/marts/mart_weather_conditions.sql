with weather as (
    select * from {{ ref('stg_ocean_weather') }}
),

-- Assign human-readable region names based on coordinates
weather_with_region as (
    select
        *,
        case
            when latitude between 62.0 and 63.0 and longitude between 4.0 and 6.0
                then 'Ulsteinvik / Sunnmore'
            when latitude between 60.0 and 61.0 and longitude between 4.5 and 6.0
                then 'Bergen / Hordaland'
            when latitude between 63.0 and 64.0 and longitude between 9.5 and 11.0
                then 'Trondheim / Trondelag'
            when latitude between 58.5 and 59.5 and longitude between 5.0 and 6.5
                then 'Stavanger / Rogaland'
            when longitude < 5.0
                then 'Offshore Norwegian Sea'
            else 'Other Norwegian Coast'
        end as region
    from weather
),

daily_conditions as (
    select
        region,
        forecast_date,

        -- Wave statistics
        round(avg(wave_height_m), 2) as avg_wave_height_m,
        round(max(wave_height_m), 2) as max_wave_height_m,
        round(min(wave_height_m), 2) as min_wave_height_m,

        -- Wind statistics
        round(avg(wind_speed_ms), 2) as avg_wind_speed_ms,
        round(avg(wind_speed_knots), 1) as avg_wind_speed_knots,
        round(max(wind_speed_ms), 2) as max_wind_speed_ms,

        -- Water temperature
        round(avg(water_temperature_c), 1) as avg_water_temp_c,

        -- Current
        round(avg(current_speed_ms), 3) as avg_current_speed_ms,

        -- Dominant sea state (most frequent)
        mode(sea_state) as dominant_sea_state,

        -- Count of forecast points
        count(*) as observation_count

    from weather_with_region
    group by region, forecast_date
)

select * from daily_conditions
