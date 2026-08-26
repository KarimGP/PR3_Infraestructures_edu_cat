-- La suma de certificacions d'un projecte no pot superar el previst
-- en mes d'un 20%. Un marge d'excés es normal (modificats d'obra,
-- revisions de preus), pero una desviacio superior indicaria un error
-- de dades o un descontrol pressupostari que caldria investigar.

with per_projecte as (
    select
        id_projecte,
        codi_projecte,
        max(import_previst)      as import_previst,
        sum(import_certificat)   as total_certificat
    from {{ ref('fct_certificacions') }}
    group by 1, 2
)

select *
from per_projecte
where total_certificat > import_previst * 1.20