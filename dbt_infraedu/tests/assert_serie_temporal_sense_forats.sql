-- La dimensio de temps ha de ser continua: cap dia pot faltar.
-- Un forat trencaria la intel·ligencia temporal de Power BI de
-- manera silenciosa (els acumulats donarien numeros erronis sense
-- cap avis).

with dies as (
    select
        data,
        lag(data) over (order by data) as dia_anterior
    from {{ ref('dim_temps') }}
)

select
    data,
    dia_anterior,
    data - dia_anterior as salt_dies
from dies
where dia_anterior is not null
  and data - dia_anterior != 1