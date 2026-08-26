-- Classificacio dels centres a partir de les banderes d'ensenyament.
--
-- La font (Directori de centres docents) NO dona un camp "tipus": dona
-- banderes independents per a cada ensenyament autoritzat. Un mateix
-- centre pot impartir infantil, primaria i ESO alhora.
--
-- La classificacio es una DECISIO NOSTRA i va aqui, no a staging, per
-- dues raons: queda documentada en un sol lloc, i es pot testar.
--
-- Ordre de les regles (el primer que encaixa guanya):
--   1. Educacio especial es una categoria a part
--   2. Centres de formacio d'adults
--   3. Si te ESO o batxillerat -> institut (encara que tingui FP)
--   4. Nomes FP -> centre de formacio professional
--   5. Primaria (amb o sense infantil) -> escola
--   6. Nomes infantil 1r cicle -> llar d'infants

with centres as (
    select * from {{ ref('stg_centres') }}
),

municipis as (
    select * from {{ ref('stg_municipis') }}
),

comarques as (
    select * from {{ ref('stg_comarques') }}
),

classificats as (
    select
        c.*,
        case
            when c.te_especial                          then 'EDUCACIO_ESPECIAL'
            when c.te_adults                            then 'ADULTS'
            when c.te_eso or c.te_batxillerat           then 'INSTITUT'
            when c.te_fp_mitja or c.te_fp_superior      then 'FORMACIO_PROFESSIONAL'
            when c.te_primaria                          then 'ESCOLA'
            when c.te_infantil_1c or c.te_infantil_2c   then 'LLAR_INFANTS'
            else 'ALTRES'
        end as tipus_centre,

        -- Quants ensenyaments diferents imparteix. Un centre que en fa
        -- molts es mes complex de mantenir.
        (c.te_infantil_1c::int + c.te_infantil_2c::int + c.te_primaria::int
         + c.te_eso::int + c.te_batxillerat::int + c.te_fp_mitja::int
         + c.te_fp_superior::int + c.te_adults::int + c.te_especial::int)
            as num_ensenyaments,

        2026 - c.any_construccio as antiguitat_anys,

        case
            when c.num_alumnes is null      then null
            when c.num_alumnes < 100        then 'PETIT'
            when c.num_alumnes < 400        then 'MITJA'
            when c.num_alumnes < 800        then 'GRAN'
            else 'MOLT_GRAN'
        end as tram_mida
    from centres c
)

select
    cl.codi_centre,
    cl.denominacio,
    cl.tipus_centre,
    cl.naturalesa,
    cl.titularitat,
    cl.num_ensenyaments,
    -- Geografia desnormalitzada: la dimensio de centre ha de portar
    -- tot el context territorial per evitar joins a Power BI.
    cl.codi_ine,
    m.nom_municipi,
    m.codi_comarca,
    co.nom_comarca,
    co.provincia,
    cl.adreca,
    cl.codi_postal,
    cl.latitud,
    cl.longitud,
    cl.te_infantil_1c,
    cl.te_infantil_2c,
    cl.te_primaria,
    cl.te_eso,
    cl.te_batxillerat,
    cl.te_fp_mitja,
    cl.te_fp_superior,
    cl.te_adults,
    cl.te_especial,
    cl.any_construccio,
    cl.antiguitat_anys,
    cl.superficie_m2,
    cl.num_alumnes,
    cl.tram_mida,
    cl.estat_conservacio
from classificats cl
join municipis m  on m.codi_ine = cl.codi_ine
join comarques co on co.codi_comarca = m.codi_comarca