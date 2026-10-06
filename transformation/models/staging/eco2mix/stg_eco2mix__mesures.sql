with source as (

    select * from {{ source('eco2mix', 'eco2mix_national') }}

),

renamed as (

    select
        -- horodatage
        cast(date_heure as timestamp)                             as horodatage_utc,
        datetime(cast(date_heure as timestamp), 'Europe/Paris')   as horodatage_paris,

        -- indicateurs principaux
        consommation                                              as consommation_mw,
        taux_co2                                                  as intensite_co2_g_kwh,

        -- production par filière (MW)
        nucleaire                                                 as production_nucleaire_mw,
        eolien                                                    as production_eolien_mw,
        solaire                                                   as production_solaire_mw,
        hydraulique                                               as production_hydraulique_mw,
        bioenergies                                               as production_bioenergies_mw,
        gaz                                                       as production_gaz_mw,
        charbon                                                   as production_charbon_mw,
        fioul                                                     as production_fioul_mw,
        pompage                                                   as pompage_mw,
        ech_physiques                                             as echanges_physiques_mw,

        -- métadonnées de chargement
        _extracted_at

    from source

),

deduplicated as (

    
    select *
    from renamed
    where intensite_co2_g_kwh is not null
    qualify row_number() over (partition by horodatage_utc order by _extracted_at desc) = 1

)

select * from deduplicated