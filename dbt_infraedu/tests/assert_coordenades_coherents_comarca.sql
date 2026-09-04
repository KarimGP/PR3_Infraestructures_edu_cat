-- Les coordenades han de ser coherents amb la comarca del centre.
--
-- La font (Directori de centres docents de la Generalitat) conte
-- centres mal geolocalitzats: es van detectar dos casos mirant un
-- mapa que no quadrava (un institut de Barcelona situat al Penedes i
-- una escola de l'Hospitalet al Pirineu). Son pocs sobre 5.434, pero
-- distorsionen visiblement qualsevol mapa perque arrosseguen totes
-- les incidencies del centre a una ubicacio erronia.
--
-- Es un defecte de la FONT, no del pipeline. El test no l'arregla:
-- el fa visible i evita que passi desapercebut si la font n'incorpora
-- mes en futures actualitzacions.
--
-- Metode: per a cada comarca amb prou centres, calculem el centroide
-- i marquem els que se n'allunyen mes de 0,5 graus (uns 50 km).
{{ config(severity='warn', warn_if='>0', error_if='>50') }}
with centroides as (
    select
        nom_comarca,
        avg(latitud)  as lat_centre,
        avg(longitud) as lon_centre
    from {{ ref('dim_centre') }}
    where latitud is not null
    group by 1
    having count(*) >= 10
)

select
    c.codi_centre,
    c.denominacio,
    c.nom_municipi,
    c.nom_comarca,
    round(c.latitud, 4)  as latitud,
    round(c.longitud, 4) as longitud,
    round(ct.lat_centre::numeric, 4) as lat_esperada,
    round(ct.lon_centre::numeric, 4) as lon_esperada
from {{ ref('dim_centre') }} c
join centroides ct on ct.nom_comarca = c.nom_comarca
where abs(c.latitud - ct.lat_centre)  > 0.5
   or abs(c.longitud - ct.lon_centre) > 0.5