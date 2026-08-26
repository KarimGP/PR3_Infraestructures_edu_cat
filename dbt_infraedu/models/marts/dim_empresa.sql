-- DIMENSIO EMPRESA
--
-- Gra: un registre per empresa adjudicataria.
-- El lot territorial ve del contracte, no de l'empresa: una empresa
-- podria tenir mes d'un lot, encara que en aquest cas no passi.

with empreses as (
    select * from {{ ref('stg_empreses') }}
),

contractes as (
    select
        nif_empresa,
        lot,
        codi_expedient,
        import_adjudicat
    from {{ source('ops', 'contractes') }}
)

select
    e.nif_empresa,
    e.nom_empresa,
    e.tipus_empresa,
    c.lot,
    c.codi_expedient,
    c.import_adjudicat
from empreses e
left join contractes c on c.nif_empresa = e.nif_empresa