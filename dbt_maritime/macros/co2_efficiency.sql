{% macro co2_per_nautical_mile(co2_col, distance_col) %}
    case
        when {{ distance_col }} > 0
        then round({{ co2_col }} * 1000 / {{ distance_col }}, 2)
        else null
    end
{% endmacro %}

{% macro co2_per_dwt_nm(co2_col, distance_col, dwt_col) %}
    case
        when {{ distance_col }} > 0 and {{ dwt_col }} > 0
        then round(({{ co2_col }} * 1000000) / ({{ distance_col }} * {{ dwt_col }}), 4)
        else null
    end
{% endmacro %}
