-- DIMENSIO TEMPS
--
-- Genera un registre per dia des de l'inici de les dades fins al final
-- del periode. Sense una dimensio de temps, Power BI no pot fer
-- intel·ligencia temporal (acumulats anuals, comparacions any contra
-- any, mitjanes mobils): necessita una taula de dates continua i
-- completa, sense forats.
--
-- Aixo es important: si generessis les dates a partir de les que
-- apareixen als fets, els dies sense cap incidencia no existirien i
-- els grafics tindrien salts.
--
-- El calendari escolar (curs de setembre a agost) es propi del domini
-- educatiu i no el dona cap funcio estandard.

{% set data_inici = '2023-09-01' %}
{% set data_fi = '2026-12-31' %}

with dies as (
    select generate_series(
        '{{ data_inici }}'::date,
        '{{ data_fi }}'::date,
        interval '1 day'
    )::date as data
)

select
    data,
    -- Clau numerica en format YYYYMMDD. Es la convencio habitual a les
    -- dimensions de temps: ordena be i ocupa menys que una data.
    to_char(data, 'YYYYMMDD')::int      as id_data,

    extract(year   from data)::int      as any,
    extract(month  from data)::int      as mes,
    extract(day    from data)::int      as dia,
    extract(quarter from data)::int     as trimestre,
    extract(week   from data)::int      as setmana_any,
    extract(isodow from data)::int      as dia_setmana,

    to_char(data, 'TMMonth')            as nom_mes,
    to_char(data, 'TMDay')              as nom_dia,
    to_char(data, 'YYYY-MM')            as any_mes,

    extract(isodow from data) in (6, 7) as es_cap_setmana,

    -- Curs escolar: de setembre a agost. Una incidencia del novembre
    -- de 2024 pertany al curs 2024/2025, no al 2024.
    case
        when extract(month from data) >= 9
            then extract(year from data)::int
        else extract(year from data)::int - 1
    end                                 as any_curs_inici,

    case
        when extract(month from data) >= 9
            then extract(year from data)::text || '/' ||
                 (extract(year from data)::int + 1)::text
        else (extract(year from data)::int - 1)::text || '/' ||
             extract(year from data)::text
    end                                 as curs_escolar,

    -- Periode lectiu aproximat. Els centres estan tancats al juliol i
    -- a l'agost, i aixo explica la vall d'incidencies d'aquells mesos.
    extract(month from data)::int not in (7, 8) as es_periode_lectiu

from dies
