-- Cap incidencia pot resoldre's abans d'haver-se obert.
-- Sembla obvi, pero es exactament el tipus d'error que produeix un
-- generador de dades mal calibrat o una carrega amb zones horaries
-- mal gestionades. Un test retorna les files PROBLEMATIQUES: si en
-- retorna zero, passa.

select
    id_incidencia,
    data_obertura,
    data_restabliment,
    data_resolucio_definitiva
from {{ ref('fct_incidencies') }}
where data_restabliment < data_obertura
   or data_resolucio_definitiva < data_obertura
   or data_resolucio_definitiva < data_restabliment