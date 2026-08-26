-- DIMENSIO TIPUS D'INCIDENCIA
--
-- Gra: un registre per tipus d'averia.
-- Les probabilitats d'urgencia son parametres del generador sintetic,
-- no atributs operatius; es mantenen per traçabilitat de la simulacio.

select
    codi_tipus,
    familia,
    descripcio_tipus,
    cost_mitja,
    -- Classificacio de cost per facilitar l'analisi al dashboard
    case
        when cost_mitja < 150   then 'BAIX'
        when cost_mitja < 500   then 'MITJA'
        when cost_mitja < 1000  then 'ALT'
        else 'MOLT_ALT'
    end as tram_cost
from {{ ref('stg_tipus_incidencia') }}