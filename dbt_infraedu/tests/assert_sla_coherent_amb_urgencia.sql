-- El nivell d'SLA ha de derivar-se correctament de l'urgencia:
-- URGENT (24h) si i nomes si afecta la seguretat o interromp
-- l'activitat docent. NORMAL (120h) en tota la resta.
--
-- Aquest test protegeix la regla de negoci central del projecte. Si
-- algu canvies la logica de classificacio, aqui saltaria.

select
    id_incidencia,
    requereix_seguretat,
    interromp_activitat,
    codi_sla,
    sla_hores
from {{ ref('fct_incidencies') }}
where (
        (requereix_seguretat or interromp_activitat)
        and (codi_sla != 'URGENT' or sla_hores != 24)
      )
   or (
        not (requereix_seguretat or interromp_activitat)
        and (codi_sla != 'NORMAL' or sla_hores != 120)
      )