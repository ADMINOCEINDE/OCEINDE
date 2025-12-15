{% macro generate_schema_name(custom_schema_name, node) %}

    {# 
      Si tu définis explicitement un schema (ex: +schema: silver_m3),
      ALORS dbt DOIT utiliser ce schema tel quel.
    #}

    {% if custom_schema_name is not none %}
        {{ custom_schema_name | trim }}
    
    {% else %}
        {# Sinon, utilise le schéma par défaut défini dans profile.yml #}
        {{ target.schema }}
    {% endif %}

{% endmacro %}
