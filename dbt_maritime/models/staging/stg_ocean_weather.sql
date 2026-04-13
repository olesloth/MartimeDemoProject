with source as (
    select * from {{ source('raw', 'ocean_weather') }}
),

cleaned as (
    select
        cast(latitude as float) as latitude,
        cast(longitude as float) as longitude,
        cast(forecast_time as timestamp_ntz) as forecast_time,
        date_trunc('day', forecast_time) as forecast_date,

        -- Wave data
        cast(wave_height_m as float) as wave_height_m,
        cast(wave_direction_deg as float) as wave_direction_deg,
        cast(wave_period_s as float) as wave_period_s,

        -- Wind data
        cast(wind_speed_ms as float) as wind_speed_ms,
        round(wind_speed_ms * 1.94384, 1) as wind_speed_knots,
        cast(wind_direction_deg as float) as wind_direction_deg,

        -- Water data
        cast(water_temperature_c as float) as water_temperature_c,
        cast(current_speed_ms as float) as current_speed_ms,
        cast(current_direction_deg as float) as current_direction_deg,

        -- Sea state classification (Douglas scale)
        case
            when wave_height_m is null then 'Unknown'
            when wave_height_m < 0.1 then 'Calm (glassy)'
            when wave_height_m < 0.5 then 'Calm (rippled)'
            when wave_height_m < 1.25 then 'Smooth'
            when wave_height_m < 2.5 then 'Slight'
            when wave_height_m < 4.0 then 'Moderate'
            when wave_height_m < 6.0 then 'Rough'
            when wave_height_m < 9.0 then 'Very rough'
            when wave_height_m < 14.0 then 'High'
            else 'Very high'
        end as sea_state,

        loaded_at

    from source
    where
        forecast_time is not null
        and latitude is not null
        and longitude is not null
)

select * from cleaned
