{% macro split_last_and_first_name(column_name) %}
{# This macro splits a full name in the format "LASTNAME Firstname"  #}
{# into two separate fields: `nom` (last name) and `prenom` (first name).  #}
{# It assumes the last name is written in uppercase letters,   #}
{# followed by the first name in mixed case.  #}
{# Example: "LEGRAND Jean" → nom = "LEGRAND", prenom = "Jean"  #}

    (
        (regexp_match(
            {{ column_name }},
            '^([[:upper:]\-\'']+(?:\s+[[:upper:]\-\'']+)*)\s+(.+)$'
        ))[1]
    ) AS nom,
    (
        (regexp_match(
            {{ column_name }},
            '^([[:upper:]\-\'']+(?:\s+[[:upper:]\-\'']+)*)\s+(.+)$'
        ))[2]
    ) AS prenom
{% endmacro %}
