-- Comarques de Catalunya. Font REAL (Directori de centres docents).
-- Staging nomes neteja i renombra: cap logica de negoci.

with origen as (
    select * from {{ source('ops', 'comarques') }}
)

select
    codi_comarca,
    nom       as nom_comarca,
    provincia
from origen
