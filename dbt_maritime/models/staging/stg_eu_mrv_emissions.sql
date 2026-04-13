with source as (
    select * from {{ source('raw', 'eu_mrv_emissions') }}
),

cleaned as (
    select
        cast(imo_number as varchar) as imo_number,
        trim(vessel_name) as vessel_name,
        trim(ship_type) as ship_type,
        cast(reporting_period as integer) as reporting_period,
        trim(technical_efficiency) as technical_efficiency,
        trim(port_of_registry) as port_of_registry,

        -- Emissions
        cast(total_co2_emissions_mt as float) as total_co2_emissions_mt,
        cast(co2_emissions_on_laden_voyages_mt as float) as co2_emissions_laden_mt,
        cast(co2_emissions_from_all_voyages_between_ports_under_ms_jurisdiction_mt as float) as co2_between_eu_ports_mt,
        cast(co2_emissions_within_ports_under_ms_jurisdiction_at_berth_mt as float) as co2_at_berth_mt,

        -- Fuel
        cast(a_total_fuel_consumption_mt as float) as total_fuel_consumption_mt,
        cast(fuel_consumption_within_ports_at_berth_mt as float) as fuel_at_berth_mt,

        -- Operational
        cast(distance_travelled_nm as float) as distance_travelled_nm,
        cast(time_at_sea_hours as float) as time_at_sea_hours,
        cast(average_speed_knots as float) as average_speed_knots,

        -- Vessel specs
        cast(deadweight_tonnes as float) as deadweight_tonnes,
        cast(gross_tonnage as float) as gross_tonnage,

        -- Efficiency metrics
        cast(annual_average_eeoi as float) as eeoi,
        cast(annual_average_co2_emissions_per_distance_kg_per_nm as float) as co2_per_nm_kg,

        -- Monitoring methods
        monitoring_method_a,
        monitoring_method_b,
        monitoring_method_c,
        monitoring_method_d,

        loaded_at

    from source
    where
        imo_number is not null
        and total_co2_emissions_mt is not null
        and cast(total_co2_emissions_mt as float) > 0
)

select * from cleaned
