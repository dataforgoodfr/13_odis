{{ config(
    tags = ['silver', 'presentation','elu'],
    )
}}

with filtre_type_fonction as (

    select
        prenom_de_l_elu, 
	    nom_de_l_elu,
	    libelle_de_la_fonction,
	    filename as type_de_la_fonction,
	    date_de_debut_du_mandat,
	    date_de_debut_de_la_fonction,
	    code_sexe,
	    reg_code,
	    dep_code,
        com_code,
	    reg_name,
	    dep_name,
        com_name
    from {{ ref ("presentation_elus_communes") }}
    where filename in ('Départemental','Municipal','Régional')
)


select * from filtre_type_fonction