-- Incidencies de manteniment.
--
-- Aqui NOMES tipem i renombrem. El calcul de temps de resolucio i
-- compliment d'SLA va a la capa intermediate: son logica de negoci.

with origen as (
    select * from {{ source('ops', 'incidencies') }}
)

select
    id_incidencia,
    uuid_origen,
    codi_centre,
    codi_tipus,
    requereix_seguretat,
    interromp_activitat,
    estat,
    canal_entrada,
    data_obertura,
    data_assignacio,
    data_resolucio_provisional,
    data_resolucio_definitiva,
    cost_estimat
from origen
where estat != 'ANULADA'