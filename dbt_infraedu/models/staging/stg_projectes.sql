-- Projectes d'inversio plurianual per centre.

with origen as (
    select * from {{ source('ops', 'projectes_inversio') }}
)

select
    id_projecte,
    codi_projecte,
    codi_centre,
    denominacio      as denominacio_projecte,
    tipologia,
    any_inici,
    any_previst_fi,
    import_previst,
    estat            as estat_projecte
from origen