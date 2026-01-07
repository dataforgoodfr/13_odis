-- Test that we can apply the table de passage on the most recent year - "on itself"
select
    "Ancien Code Officiel",
    "Code Courant Officiel"
from {{ ref('geocodes_passage_annuel') }}
where
    "Code Courant Officiel" not in (
        select distinct "Ancien Code Officiel"
        from {{ ref('geocodes_passage_annuel') }}
    )