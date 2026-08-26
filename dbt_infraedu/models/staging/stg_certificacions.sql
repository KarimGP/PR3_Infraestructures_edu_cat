-- Certificacions: execucio pressupostaria real.

with origen as (
    select * from {{ source('ops', 'certificacions') }}
)

select
    id_certificacio,
    id_projecte,
    num_certificacio,
    data_certificacio,
    exercici,
    import_certificat
from origen