with source as (
    select * from {{ source('raw', 'ais_positions') }}
),

cleaned as (
    select
        cast(mmsi as varchar) as mmsi,
        cast(imo_number as varchar) as imo_number,
        trim(vessel_name) as vessel_name,
        cast(ship_type as integer) as ship_type_code,
        cast(latitude as float) as latitude,
        cast(longitude as float) as longitude,
        cast(speed_knots as float) as speed_knots,
        cast(course as float) as course,
        cast(heading as float) as heading,
        trim(destination) as destination,
        cast(navigation_status as integer) as navigation_status,
        received_at,
        loaded_at
    from source
    where
        -- Filter invalid coordinates
        latitude between -90 and 90
        and longitude between -180 and 180
        and mmsi is not null
),

deduplicated as (
    select
        *,
        row_number() over (
            partition by mmsi, date_trunc('minute', received_at)
            order by received_at desc
        ) as row_num
    from cleaned
)

select * exclude (row_num)
from deduplicated
where row_num = 1
