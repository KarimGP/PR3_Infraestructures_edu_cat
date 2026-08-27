-- DATASET PER AL MODEL PREDICTIU
--
-- GRA: una fila per dia i familia d'averia.
--
-- Predim el nombre d'incidencies diaries per familia. Es una serie
-- temporal amb senyal real: estacionalitat de climatitzacio (pics al
-- novembre i al maig, quan s'arrenquen els sistemes) i calendari
-- escolar (vall al juliol i agost).
--
-- IMPORTANT: la serie ha de ser COMPLETA. Els dies sense cap incidencia
-- han d'apareixer amb zero, no desapareixer. Si no, el model aprendria
-- d'una serie amb forats i les mitjanes mobils serien incorrectes.
-- Per aixo fem un CROSS JOIN entre dim_temps i les families.
--
-- Els LAGS son la peça clau: el model no pot veure el futur, nomes el
-- passat. lag_1, lag_7 i lag_14 li donen memoria recent i setmanal.

with families as (
    select distinct familia from {{ ref('dim_tipus_incidencia') }}
),

calendari as (
    select *
    from {{ ref('dim_temps') }}
    where data between
        (select min(data_obertura)::date from {{ ref('fct_incidencies') }})
        and
        (select max(data_obertura)::date from {{ ref('fct_incidencies') }})
),

-- Esquelet complet: totes les combinacions dia x familia
esquelet as (
    select c.*, f.familia
    from calendari c
    cross join families f
),

recompte_real as (
    select
        f.data_obertura::date as data,
        t.familia,
        count(*)              as num_incidencies,
        sum(f.cost_real)      as cost_total,
        count(*) filter (where f.requereix_seguretat
                            or f.interromp_activitat) as num_urgents
    from {{ ref('fct_incidencies') }} f
    join {{ ref('dim_tipus_incidencia') }} t using (codi_tipus)
    group by 1, 2
),

unit as (
    select
        e.data,
        e.familia,
        e."any",
        e.mes,
        e.dia_setmana,
        e.setmana_any,
        e.trimestre,
        e.es_cap_setmana,
        e.es_periode_lectiu,
        e.curs_escolar,
        coalesce(r.num_incidencies, 0) as num_incidencies,
        coalesce(r.cost_total, 0)      as cost_total,
        coalesce(r.num_urgents, 0)     as num_urgents
    from esquelet e
    left join recompte_real r
        on r.data = e.data and r.familia = e.familia
)

select
    data,
    familia,
    "any",
    mes,
    dia_setmana,
    setmana_any,
    trimestre,
    es_cap_setmana::int    as es_cap_setmana,
    es_periode_lectiu::int as es_periode_lectiu,
    curs_escolar,

    -- Variable objectiu
    num_incidencies,
    num_urgents,
    cost_total,

    -- LAGS: nomes informacio del passat, mai del futur
    lag(num_incidencies, 1)  over (partition by familia order by data) as lag_1,
    lag(num_incidencies, 7)  over (partition by familia order by data) as lag_7,
    lag(num_incidencies, 14) over (partition by familia order by data) as lag_14,

    -- Mitjanes mobils. El ROWS BETWEEN ... AND 1 PRECEDING exclou el
    -- dia actual: si l'inclogues, el model veuria part de la resposta.
    avg(num_incidencies) over (
        partition by familia order by data
        rows between 7 preceding and 1 preceding
    ) as mitjana_mobil_7,

    avg(num_incidencies) over (
        partition by familia order by data
        rows between 30 preceding and 1 preceding
    ) as mitjana_mobil_30,

    stddev(num_incidencies) over (
        partition by familia order by data
        rows between 30 preceding and 1 preceding
    ) as desviacio_mobil_30

from unit
order by familia, data