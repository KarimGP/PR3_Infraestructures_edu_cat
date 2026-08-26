-- DIMENSIO CENTRE
--
-- Dimensio desnormalitzada: porta tot el context territorial i els
-- atributs del centre, de manera que Power BI no hagi de fer joins
-- addicionals. En un esquema en estrella, les dimensions es
-- desnormalitzen a proposit: es prioritza la velocitat de consulta
-- sobre l'estalvi d'espai.
--
-- Gra: un registre per centre educatiu.

select
    codi_centre,
    denominacio,
    tipus_centre,
    naturalesa,
    titularitat,
    num_ensenyaments,
    nom_municipi,
    nom_comarca,
    provincia,
    codi_ine,
    codi_comarca,
    adreca,
    codi_postal,
    latitud,
    longitud,
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
    antiguitat_anys,
    superficie_m2,
    num_alumnes,
    tram_mida,
    estat_conservacio
from {{ ref('int_centres_classificats') }}