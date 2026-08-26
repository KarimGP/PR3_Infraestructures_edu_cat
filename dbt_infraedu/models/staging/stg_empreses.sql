with origen as (
    select * from {{ source('ops', 'empreses') }}
)

select
    nif      as nif_empresa,
    nom      as nom_empresa,
    tipus    as tipus_empresa
from origen