-- Calcul de temps de resolucio i compliment d'SLA.
--
-- TRES DECISIONS DE NEGOCI, totes explicites:
--
-- 1. La prioritat NO es un atribut lliure de la incidencia: es deriva
--    de si afecta la seguretat o interromp l'activitat docent. Aixi es
--    prioritza realment al manteniment educatiu.
--
-- 2. L'SLA es mesura contra el RESTABLIMENT DEL SERVEI, no contra la
--    reparacio definitiva. A les urgents, el restabliment es la
--    resolucio provisional: una averia de calefaccio al gener es tapa
--    en 24h i es repara de veritat setmanes despres. A les normals no
--    hi ha fase provisional perque no cal, i la definitiva ES el
--    restabliment. Per aixo el COALESCE.
--
-- 3. Les incidencies encara obertes NO compten per al compliment.
--    Encara no han incomplert res; nomes ho sabrem quan es tanquin.
--    Comptar-les com a incompliment inflaria la metrica, i per aixo
--    compleix_sla es NULL i no FALSE.

with incidencies as (
    select * from {{ ref('stg_incidencies') }}
),

sla as (
    select * from {{ source('ops', 'sla') }}
),

classificades as (
    select
        i.*,
        case
            when i.requereix_seguretat or i.interromp_activitat then 'URGENT'
            else 'NORMAL'
        end as codi_sla
    from incidencies i
),

amb_limits as (
    select
        c.*,
        s.hores_maximes as sla_hores,
        c.data_obertura + (s.hores_maximes * interval '1 hour') as data_limit_sla,
        coalesce(c.data_resolucio_provisional,
                 c.data_resolucio_definitiva) as data_restabliment
    from classificades c
    join sla s on s.codi_sla = c.codi_sla
)

select
    id_incidencia,
    uuid_origen,
    codi_centre,
    codi_tipus,
    estat,
    canal_entrada,
    requereix_seguretat,
    interromp_activitat,
    codi_sla,
    sla_hores,
    data_obertura,
    data_assignacio,
    data_resolucio_provisional,
    data_resolucio_definitiva,
    data_restabliment,
    data_limit_sla,
    cost_estimat,

    -- Hores fins al restabliment del servei (metrica d'SLA)
    extract(epoch from (data_restabliment - data_obertura)) / 3600.0
        as hores_fins_restabliment,

    -- Hores fins a la reparacio real (metrica de qualitat del servei)
    extract(epoch from (data_resolucio_definitiva - data_obertura)) / 3600.0
        as hores_fins_definitiva,

    -- NULL vol dir "no avaluable", no "incomplert".
    case
        when data_restabliment is null then null
        when data_restabliment <= data_limit_sla then true
        else false
    end as compleix_sla,

    -- Va necessitar una segona intervencio despres del restabliment?
    case
        when data_restabliment is null then null
        when data_resolucio_definitiva is null then null
        else data_resolucio_definitiva > data_restabliment
    end as ha_calgut_reparacio_posterior,

    data_resolucio_definitiva is not null as esta_tancada

from amb_limits