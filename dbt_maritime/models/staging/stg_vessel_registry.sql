with source as (
    select * from {{ ref('ulstein_vessel_registry') }}
),

cleaned as (
    select
        cast(imo_number as varchar) as imo_number,
        trim(vessel_name) as vessel_name,
        trim(design_type) as design_type,
        trim(hull_type) as hull_type,
        cast(has_x_bow as boolean) as has_x_bow,
        cast(build_year as integer) as build_year,
        trim(owner) as owner,
        trim(ship_category) as ship_category
    from source
)

select * from cleaned
