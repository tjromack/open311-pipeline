{#
    sla_threshold_hours: render a CASE expression that maps an urgency_label
    column to its SLA threshold in hours.

    Thresholds are read from var('sla_thresholds') in dbt_project.yml so they
    can be tuned without editing SQL. Labels not present in the var (e.g.
    'Unknown') return NULL so they are excluded from SLA compliance metrics.
#}
{% macro sla_threshold_hours(urgency_label_column) %}
    case {{ urgency_label_column }}
    {% for label, hours in var('sla_thresholds').items() %}
        when '{{ label }}' then {{ hours }}
    {% endfor %}
        else null
    end
{% endmacro %}
