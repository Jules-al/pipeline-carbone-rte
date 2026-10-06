with mesures as (

    select * from {{ ref('stg_eco2mix__mesures') }}

),

par_heure as (

    -- regroupement des 4 quarts d'heure de chaque heure
    select
        timestamp_trunc(horodatage_utc, hour)     as heure_utc,
        count(*)                                  as nb_quarts_heure,

        avg(intensite_co2_g_kwh)                  as intensite_co2_g_kwh,
        avg(consommation_mw)                      as consommation_mw,

        avg(production_nucleaire_mw)              as production_nucleaire_mw,
        avg(production_eolien_mw)                 as production_eolien_mw,
        avg(production_solaire_mw)                as production_solaire_mw,
        avg(production_hydraulique_mw)            as production_hydraulique_mw,
        avg(production_bioenergies_mw)            as production_bioenergies_mw,
        avg(production_gaz_mw)                    as production_gaz_mw,
        avg(production_charbon_mw)                as production_charbon_mw,
        avg(production_fioul_mw)                  as production_fioul_mw

    from mesures
    group by heure_utc

),

enrichi as (

    select
        heure_utc,
        datetime(heure_utc, 'Europe/Paris')                                  as heure_paris,
        date(heure_utc, 'Europe/Paris')                                      as date_paris,
        extract(hour from datetime(heure_utc, 'Europe/Paris'))               as heure_de_la_journee,

        -- 1 = lundi … 7 = dimanche (BigQuery renvoie 1 = dimanche par défaut)
        mod(extract(dayofweek from datetime(heure_utc, 'Europe/Paris')) + 5, 7) + 1
                                                                             as jour_semaine_num,

        nb_quarts_heure,
        nb_quarts_heure = 4                                                  as est_heure_complete,

        intensite_co2_g_kwh,
        consommation_mw,

        production_nucleaire_mw + production_eolien_mw + production_solaire_mw
            + production_hydraulique_mw + production_bioenergies_mw
            + production_gaz_mw + production_charbon_mw + production_fioul_mw
                                                                             as production_totale_mw,
        production_gaz_mw + production_charbon_mw + production_fioul_mw      as production_fossile_mw,
        production_eolien_mw + production_solaire_mw
            + production_hydraulique_mw + production_bioenergies_mw          as production_renouvelable_mw,
        production_nucleaire_mw

    from par_heure

)

select
    *,
    case jour_semaine_num
        when 1 then 'Lundi'    when 2 then 'Mardi'  when 3 then 'Mercredi'
        when 4 then 'Jeudi'    when 5 then 'Vendredi'
        when 6 then 'Samedi'   when 7 then 'Dimanche'
    end                                                                      as jour_semaine,
    jour_semaine_num >= 6                                                    as est_weekend,
    safe_divide(production_fossile_mw, production_totale_mw)                 as part_fossile,
    safe_divide(production_renouvelable_mw, production_totale_mw)            as part_renouvelable,
    safe_divide(production_nucleaire_mw, production_totale_mw)               as part_nucleaire
from enrichi