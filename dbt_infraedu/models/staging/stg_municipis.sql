-- Municipis amb codi INE. Font REAL.

with origen as (
    select * from {{ source('ops', 'municipis') }}
)

select
    codi_ine,
    nom          as nom_municipi,
    codi_comarca,
    poblacio
from origen
