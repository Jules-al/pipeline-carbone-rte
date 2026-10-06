-- Série horaire de l'intensité carbone : base des courbes du dashboard
select
    *,
    rank() over (partition by date_paris order by intensite_co2_g_kwh) as rang_dans_la_journee
from {{ ref('int_eco2mix__horaire') }}
where est_heure_complete

