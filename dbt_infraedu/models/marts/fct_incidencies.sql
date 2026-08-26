-- FET INCIDENCIES
--
-- GRA: una fila per incidencia. Aquesta es la decisio mes important
-- del modelat: defineix que representa una fila i, per tant, que
-- significa comptar-les o sumar-ne les mesures.
--
-- Les claus forasteres apunten a les dimensions. Les mesures son els
-- valors numerics agregables: cost, hores de resolucio, i banderes
-- convertides a 0/1 per poder-les sumar directament a Power BI.
--
-- Nota sobre compleix_sla: es manté com a BOOLEAN amb NULL per a les
-- no avaluables, PERO tambe s'exposa com a enter (1/0/NULL) perque el
-- DAX de Power BI pugui sumar-lo sense conversions.

with incidencies as (
    select * from {{ ref('int_incidencies_sla') }}
),

-- Cost real agregat des de les actuacions. El cost_estimat de la
-- incidencia es una previsio; el que val es el que s'ha gastat.
costos_reals as (
    select
        id_incidencia,
        count(*)              as num_actuacions,
        sum(hores_treball)    as hores_treball_total,
        sum(cost_ma_obra)     as cost_ma_obra_real,
        sum(cost_materials)   as cost_materials_real,
        sum(cost_total)       as cost_real
    from {{ ref('stg_actuacions') }}
    group by 1
),

-- Quina empresa hi va intervenir. Si n'hi va haver mes d'una (cas
-- rar), agafem la de la primera actuacio.
empresa_principal as (
    select distinct on (id_incidencia)
        id_incidencia,
        nif_empresa
    from {{ ref('stg_actuacions') }}
    order by id_incidencia, data_actuacio
)

select
    -- Clau del fet
    i.id_incidencia,
    i.uuid_origen,

    -- Claus forasteres cap a les dimensions
    i.codi_centre,
    i.codi_tipus,
    ep.nif_empresa,
    to_char(i.data_obertura, 'YYYYMMDD')::int      as id_data_obertura,
    to_char(i.data_restabliment, 'YYYYMMDD')::int  as id_data_restabliment,

    -- Atributs degenerats (viuen al fet perque no justifiquen dimensio)
    i.estat,
    i.canal_entrada,
    i.codi_sla,

    -- Dates completes, per si cal el detall horari
    i.data_obertura,
    i.data_assignacio,
    i.data_restabliment,
    i.data_resolucio_definitiva,
    i.data_limit_sla,

    -- MESURES
    1                                    as num_incidencies,
    i.cost_estimat,
    coalesce(cr.cost_real, 0)            as cost_real,
    coalesce(cr.cost_ma_obra_real, 0)    as cost_ma_obra,
    coalesce(cr.cost_materials_real, 0)  as cost_materials,
    coalesce(cr.num_actuacions, 0)       as num_actuacions,
    coalesce(cr.hores_treball_total, 0)  as hores_treball,
    i.hores_fins_restabliment,
    i.hores_fins_definitiva,
    i.sla_hores,

    -- Banderes
    i.requereix_seguretat,
    i.interromp_activitat,
    i.compleix_sla,
    i.ha_calgut_reparacio_posterior,
    i.esta_tancada,

    -- Versions enteres per facilitar el DAX
    case when i.compleix_sla then 1 when i.compleix_sla is false then 0 end
        as compleix_sla_int,
    case when i.esta_tancada then 1 else 0 end
        as esta_tancada_int

from incidencies i
left join costos_reals cr     on cr.id_incidencia = i.id_incidencia
left join empresa_principal ep on ep.id_incidencia = i.id_incidencia