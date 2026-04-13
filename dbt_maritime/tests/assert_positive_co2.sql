-- Ensure all emissions records have positive CO2 values
select
    imo_number,
    reporting_period,
    total_co2_emissions_mt
from {{ ref('stg_eu_mrv_emissions') }}
where total_co2_emissions_mt <= 0
