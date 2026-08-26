-- Centres educatius. Els camps d'ubicacio i ensenyaments son REALS;
-- mida, antiguitat i estat de conservacio son SINTETICS.
--
-- Nota: NO classifiquem el centre en INS/CEIP/etc aqui. Les banderes
-- d'ensenyament es guarden tal com venen de la font i la classificacio
-- es fa a la capa intermediate, on queda documentada i testada.

with origen as (
    select * from {{ source('ops', 'centres') }}
)

select
    codi_centre,
    denominacio,
    nom_naturalesa       as naturalesa,
    nom_titularitat      as titularitat,
    codi_ine,
    adreca,
    codi_postal,
    latitud,
    longitud,
    -- Ensenyaments autoritzats (reals)
    te_infantil_1c,
    te_infantil_2c,
    te_primaria,
    te_eso,
    te_batxillerat,
    te_fp_mitja,
    te_fp_superior,
    te_adults,
    te_especial,
    any_construccio,
    superficie_m2,
    num_alumnes,
    estat_conservacio,
    curs_font,
    actiu
from origen
where actiu