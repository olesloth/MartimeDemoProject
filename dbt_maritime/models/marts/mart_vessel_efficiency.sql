with emissions as (
    select * from {{ ref('stg_eu_mrv_emissions') }}
),

registry as (
    select * from {{ ref('stg_vessel_registry') }}
),

vessel_efficiency as (
    select
        e.imo_number,
        e.vessel_name,
        e.ship_type,
        e.reporting_period,

        -- Ulstein vessel flags
        r.imo_number is not null as is_ulstein_design,
        coalesce(r.design_type, 'Unknown') as design_type,
        coalesce(r.hull_type, 'Conventional') as hull_type,
        coalesce(r.has_x_bow, false) as has_x_bow,
        r.ship_category,

        -- Raw metrics
        e.total_co2_emissions_mt,
        e.total_fuel_consumption_mt,
        e.distance_travelled_nm,
        e.time_at_sea_hours,
        e.average_speed_knots,
        e.deadweight_tonnes,
        e.gross_tonnage,
        e.eeoi,

        -- Calculated efficiency metrics
        {{ co2_per_nautical_mile('e.total_co2_emissions_mt', 'e.distance_travelled_nm') }} as co2_kg_per_nm,
        {{ co2_per_dwt_nm('e.total_co2_emissions_mt', 'e.distance_travelled_nm', 'e.deadweight_tonnes') }} as co2_g_per_dwt_nm,

        -- Fuel efficiency
        case
            when e.distance_travelled_nm > 0
            then round(e.total_fuel_consumption_mt * 1000 / e.distance_travelled_nm, 2)
            else null
        end as fuel_kg_per_nm,

        -- Efficiency ranking within ship type and year
        percent_rank() over (
            partition by e.ship_type, e.reporting_period
            order by case when e.distance_travelled_nm > 0
                then e.total_co2_emissions_mt / e.distance_travelled_nm
                else null end
        ) as efficiency_rank_pct

    from emissions e
    left join registry r on e.imo_number = r.imo_number
)

select * from vessel_efficiency
