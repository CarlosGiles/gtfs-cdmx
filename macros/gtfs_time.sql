{% macro gtfs_time_to_seconds(time_str) -%}
    /*
      Convierte 'HH:MM:SS' (incluye casos > 24h) a segundos desde medianoche.
      Retorna INTEGER o NULL si la cadena es inválida.
    */
    case
      when {{ time_str }} is null then null
      when regexp_like({{ time_str }}, '^[0-9]{1,2}:[0-5][0-9]:[0-5][0-9]$') then
        split_part({{ time_str }}, ':', 1)::int * 3600
      + split_part({{ time_str }}, ':', 2)::int * 60
      + split_part({{ time_str }}, ':', 3)::int
      /* Soporta HH >= 24 */
      when regexp_like({{ time_str }}, '^[0-9]{2,}:[0-5][0-9]:[0-5][0-9]$') then
        split_part({{ time_str }}, ':', 1)::int * 3600
      + split_part({{ time_str }}, ':', 2)::int * 60
      + split_part({{ time_str }}, ':', 3)::int
      else null
    end
{%- endmacro %}
