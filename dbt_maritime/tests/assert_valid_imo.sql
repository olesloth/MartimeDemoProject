-- IMO numbers should be 7 digits
select
    imo_number,
    vessel_name
from {{ ref('stg_eu_mrv_emissions') }}
where length(imo_number) != 7
    or try_cast(imo_number as integer) is null
