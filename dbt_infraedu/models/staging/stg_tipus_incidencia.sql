-- Cataleg de tipus d'averia.

with origen as (
    select * from {{ source('ops', 'tipus_incidencia') }}
)

select
    codi_tipus,
    familia,
    descripcio       as descripcio_tipus,
    prob_seguretat,
    prob_interrupcio,
    pes_relatiu,
    cost_mitja
from origen
