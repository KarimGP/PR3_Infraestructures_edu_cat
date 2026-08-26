-- Esdeveniments del stream. Extreiem els camps del JSONB a columnes
-- perque els models de dalt no hagin de coneixer l'estructura del
-- payload. Aixo es neteja, no logica de negoci.

with origen as (
    select * from {{ source('bronze', 'raw_events') }}
)

select
    event_id,
    event_type,
    event_ts,
    codi_centre,
    payload ->> 'magnitud'                    as magnitud,
    (payload ->> 'valor')::numeric            as valor,
    payload ->> 'unitat'                      as unitat,
    payload ->> 'codi_tipus'                  as codi_tipus,
    kafka_partition,
    kafka_offset,
    ingested_at
from origen