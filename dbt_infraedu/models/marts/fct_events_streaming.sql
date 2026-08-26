-- FET EVENTS DE STREAMING
--
-- GRA: una fila per finestra de 5 minuts, centre i tipus d'esdeveniment.
-- Ve directament de l'agregacio que fa Flink a bronze.agg_events_5min.
--
-- LIMITACIO CONEGUDA: l'agregacio de Flink agrupa per event_type pero
-- no per magnitud, de manera que valor_mitja i valor_max barregen
-- temperatures, consums i CO2 quan un centre emet lectures de
-- magnituds diferents dins la mateixa finestra. La mesura fiable es
-- num_events. Documentat al README.

with agregats as (
    select * from {{ source('bronze', 'agg_events_5min') }}
)

select
    a.finestra_inici,
    a.finestra_fi,
    a.codi_centre,
    a.event_type,
    to_char(a.finestra_inici, 'YYYYMMDD')::int as id_data,

    -- MESURES
    a.num_events,
    a.valor_mitja,
    a.valor_max,
    a.processed_at

from agregats a