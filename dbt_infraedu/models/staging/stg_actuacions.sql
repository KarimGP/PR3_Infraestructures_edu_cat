-- Visites d'operaris. El cost total es la suma de ma d'obra i
-- materials: es una suma aritmetica directa, no una regla de negoci.

with origen as (
    select * from {{ source('ops', 'actuacions') }}
)

select
    id_actuacio,
    id_incidencia,
    nif_empresa,
    data_actuacio,
    hores_treball,
    cost_ma_obra,
    cost_materials,
    cost_ma_obra + cost_materials as cost_total,
    resolt
from origen