-- FET CERTIFICACIONS
--
-- GRA: una fila per certificacio d'obra.
--
-- Aquesta taula respon la pregunta estrella de qualsevol quadre de
-- comandament d'inversio publica: quant s'ha executat realment del que
-- estava previst. La diferencia entre import_previst i la suma de
-- certificacions es la desviacio pressupostaria.
--
-- import_previst es un atribut del PROJECTE, no de la certificacio.
-- El portem al fet per comoditat, pero cal anar amb compte: sumar-lo
-- directament el multiplicaria pel nombre de certificacions. Per aixo
-- s'exposa tambe import_previst_prorratejat.

with certificacions as (
    select * from {{ ref('stg_certificacions') }}
),

projectes as (
    select * from {{ ref('stg_projectes') }}
),

-- Quantes certificacions te cada projecte, per poder prorratejar
num_cert as (
    select id_projecte, count(*) as n
    from certificacions
    group by 1
)

select
    c.id_certificacio,
    c.id_projecte,
    p.codi_projecte,
    p.codi_centre,
    to_char(c.data_certificacio, 'YYYYMMDD')::int as id_data_certificacio,

    -- Atributs del projecte
    p.denominacio_projecte,
    p.tipologia,
    p.estat_projecte,
    p.any_inici,
    p.any_previst_fi,

    c.num_certificacio,
    c.data_certificacio,
    c.exercici,

    -- MESURES
    c.import_certificat,
    -- ATENCIO: import_previst es del projecte. Sumar-lo aqui el
    -- multiplicaria per cada certificacio. Es prorrateja per poder
    -- fer sumes correctes.
    p.import_previst,
    round(p.import_previst / nc.n, 2) as import_previst_prorratejat,
    1 as num_certificacions

from certificacions c
join projectes p  on p.id_projecte = c.id_projecte
join num_cert nc  on nc.id_projecte = c.id_projecte