with vessel_efficiency as (
    select * from {{ ref('mart_vessel_efficiency') }}
),

fleet_stats as (
    select
        ship_type,
        hull_type,
        reporting_period,

        count(distinct imo_number) as vessel_count,
        count(distinct case when is_ulstein_design then imo_number end) as ulstein_vessel_count,
        count(distinct case when has_x_bow then imo_number end) as x_bow_vessel_count,

        -- CO2 metrics
        round(sum(total_co2_emissions_mt), 0) as total_co2_mt,
        round(avg(total_co2_emissions_mt), 1) as avg_co2_per_vessel_mt,
        round(avg(co2_kg_per_nm), 2) as avg_co2_kg_per_nm,
        round(median(co2_kg_per_nm), 2) as median_co2_kg_per_nm,

        -- Fuel metrics
        round(sum(total_fuel_consumption_mt), 0) as total_fuel_mt,
        round(avg(fuel_kg_per_nm), 2) as avg_fuel_kg_per_nm,

        -- Operational metrics
        round(sum(distance_travelled_nm), 0) as total_distance_nm,
        round(avg(average_speed_knots), 1) as avg_speed_knots,
        round(sum(time_at_sea_hours), 0) as total_time_at_sea_hours,

        -- Efficiency
        round(avg(eeoi), 6) as avg_eeoi,
        round(avg(efficiency_rank_pct), 3) as avg_efficiency_rank

    from vessel_efficiency
    where distance_travelled_nm > 0
    group by ship_type, hull_type, reporting_period
)

select * from fleet_stats
