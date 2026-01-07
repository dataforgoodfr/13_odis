{{ config(
    tags = ['silver', 'presentation','elu'],
    alias = 'silver_presentation_elus'
    )
}}

with ajout_nuance_par_codego as (
    select
        elus.com_code,
        elus.dep_code,
        elus.reg_code,
        elus.prenom_de_l_elu,
        elus.nom_de_l_elu,
        elus.libelle_de_la_fonction,
        case
            when elus.libelle_de_la_fonction = 'Maire' 
                then nuance_com.libelle_nuance
            when elus.libelle_de_la_fonction = 'Président du conseil départemental' 
                then nuance_dep.libelle_nuance
            when elus.libelle_de_la_fonction = 'Président du conseil régional' 
                then nuance_reg.libelle_nuance
        end as libelle_nuance
    from {{ ref ("silver_presentation_elus_communes") }} as elus
    left join {{ ref ("silver_presentation_dim_nuance_politique_reg") }} as nuance_reg
        on elus.nom_de_l_elu = nuance_reg.nom
        and elus.prenom_de_l_elu = nuance_reg.prenom
        and elus.reg_code = nuance_reg.code_officiel_region
        and elus.libelle_de_la_fonction = 'Président du conseil régional'
    left join {{ ref ("silver_presentation_dim_nuance_politique_dep") }} as nuance_dep
        on elus.nom_de_l_elu = nuance_dep.nom
        and elus.prenom_de_l_elu = nuance_dep.prenom
        and elus.dep_code = nuance_dep.code_officiel_du_departement
        and elus.libelle_de_la_fonction = 'Président du conseil départemental'
    left join {{ ref ("silver_presentation_dim_nuance_politique_com") }} as nuance_com
        on elus.nom_de_l_elu = nuance_com.nom
        and elus.prenom_de_l_elu = nuance_com.prenom
        and elus.com_code = nuance_com.code_officiel_commune
        and elus.libelle_de_la_fonction = 'Maire'
    where
        elus.libelle_de_la_fonction in (
        'Maire',
        'Président du conseil départemental',
        'Président du conseil régional'
        )
)

select * from ajout_nuance_par_codego
