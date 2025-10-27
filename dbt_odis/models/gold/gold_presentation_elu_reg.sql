{{ config(
    tags = ['gold', 'presentation','elu'],
    )
}}

with renommage_et_ajout_nuance as (
    select
        presentation_elus_communes.reg_code as "Code de la région",
        presentation_elus_communes.reg_name as "Nom de la région",
        presentation_elus_communes.prenom_de_l_elu as "Prénom de l’élu",
        presentation_elus_communes.nom_de_l_elu as "Nom de l’élu",
        presentation_elus_communes.libelle_de_la_fonction as "Nom de la fonction",
        nuance_politique.libelle_nuance as "Libellé de la nuance du mandat",
        presentation_elus_communes.type_de_la_fonction as "Type de la fonction",
        presentation_elus_communes.date_de_debut_du_mandat as "Date de début du mandat",
        presentation_elus_communes.date_de_debut_de_la_fonction as "Date de début de la fonction",
        presentation_elus_communes.code_sexe as "Genre de l’élu"
    from {{ ref ("silver_presentation_elus_communes") }} as presentation_elus_communes
    left join {{ ref ("silver_presentation_dim_nuance_politique_reg") }} as nuance_politique
        on presentation_elus_communes.nom_de_l_elu = nuance_politique.nom
        and presentation_elus_communes.prenom_de_l_elu = nuance_politique.prenom
        and presentation_elus_communes.reg_code = nuance_politique.code_officiel_region
    where type_de_la_fonction = 'Régional'
    and libelle_de_la_fonction = 'Président du conseil régional'
)

select * from renommage_et_ajout_nuance
