-- Profil moyen par jour de semaine et par heure : à quel moment consommer ?
with horaire as (

    select * from {{ ref('fct_intensite_carbone_horaire') }}

),

profil as (

    select
        jour_semaine_num,
        jour_semaine,
        heure_de_la_journee,
        count(*)                                  as nb_observations,
        round(avg(intensite_co2_g_kwh), 1)        as intensite_moyenne_g_kwh,
        round(min(intensite_co2_g_kwh), 1)        as intensite_min_g_kwh,
        round(max(intensite_co2_g_kwh), 1)        as intensite_max_g_kwh,
        round(avg(part_fossile), 3)               as part_fossile_moyenne
    from horaire
    group by jour_semaine_num, jour_semaine, heure_de_la_journee

)

select
    *,
    rank() over (partition by jour_semaine_num order by intensite_moyenne_g_kwh)   as rang_dans_le_jour,
    rank() over (partition by jour_semaine_num order by intensite_moyenne_g_kwh) <= 3
                                                                                    as est_heure_recommandee,
    round(
        safe_divide(
            intensite_moyenne_g_kwh,
            avg(intensite_moyenne_g_kwh) over (partition by jour_semaine_num)
        ) - 1,
        3
    )                                                                               as ecart_vs_moyenne_du_jour
from profil