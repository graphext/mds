{#
    Calculates the timedelta between two timestamps.

    Parameters:
    start_time: First timestamp.
    end_time: Second timestamp.
    decimals: Number of decimals to be round.
    time_units: 's' to indicate time_delta in seconds, 'm' in minutes, 'h' in hours and 'd' in days.
#}

{% macro time_delta_minutes(start_time, end_time, decimals=2, time_units = 'm') %}
    {% if time_units == 's' %}
        ROUND((EXTRACT(EPOCH FROM ({{ end_time }} - {{ start_time }})))::numeric, {{ decimals }})
    {% elif time_units == 'm' %}
        ROUND((EXTRACT(EPOCH FROM ({{ end_time }} - {{ start_time }})) / 60)::numeric, {{ decimals }})
    {% elif time_units == 'h' %}
        ROUND((EXTRACT(EPOCH FROM ({{ end_time }} - {{ start_time }})) / 3600)::numeric, {{ decimals }})
    {% elif time_units == 'd' %}
        ROUND((EXTRACT(EPOCH FROM ({{ end_time }} - {{ start_time }})) / 86400)::numeric, {{ decimals }})
    {% elif time_units == 'month' %}
        ROUND((EXTRACT(EPOCH FROM ({{ end_time }} - {{ start_time }})) / (86400 * 30.4167)::numeric, {{ decimals }})
    {% endif %}
{% endmacro %}