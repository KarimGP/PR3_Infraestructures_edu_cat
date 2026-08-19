"""
PROVA DE FOC · Flink → PostgreSQL
=================================

Objectiu: validar la peça de MÀXIM risc del projecte de manera aïllada,
el primer dia, abans d'invertir hores en res més.

Aquest job NO fa servir Kafka. Fa servir el connector `datagen`, que ve
integrat a Flink i no necessita cap JAR extern. Així, si això falla,
sabràs amb certesa que el problema és una d'aquestes quatre coses:

  1. Els JARs del connector JDBC / driver de Postgres no s'han carregat
  2. El TaskManager no està registrat al JobManager
  3. Flink no arriba a `postgres:5432` per la xarxa Docker
  4. Credencials o nom de taula incorrectes

...i cap altra. Si en canvi ho proves directament amb Kafka pel mig,
tens el doble de superfície d'error i el triple de temps de diagnòstic.

EXECUCIÓ
--------
    docker exec -it pr3-flink-jm ./bin/flink run -py /opt/flink_jobs/00_smoke_jdbc.py

VERIFICACIÓ
-----------
    docker exec -it pr3-postgres psql -U pr3 -d infraedu \
        -c "SELECT count(*), max(id) FROM bronze.smoke_test;"

    Esperat: 20 files, id màxim = 20.

Si això funciona el dia 1, la resta del pipeline de streaming és
construir a sobre d'uns fonaments que ja saps que aguanten.
"""

import os
import sys

from pyflink.table import EnvironmentSettings, TableEnvironment


def main() -> None:
    # Mode streaming encara que la font sigui fitada: volem exercitar
    # exactament el mateix camí de codi que faran servir els jobs reals.
    t_env = TableEnvironment.create(EnvironmentSettings.in_streaming_mode())
    t_env.get_config().set("parallelism.default", "1")

    # Llegim la configuració de l'entorn del contenidor (definida al
    # docker-compose.yml). Mai credencials en dur dins del codi.
    try:
        pg_url = os.environ["PG_URL"]
        pg_user = os.environ["PG_USER"]
        pg_password = os.environ["PG_PASSWORD"]
    except KeyError as e:
        sys.exit(
            f"Falta la variable d'entorn {e}. Comprova que el servei "
            f"flink-jobmanager del docker-compose.yml la defineix."
        )

    # ── Font sintètica ──────────────────────────────────────────────
    # 'sequence' genera del 1 al 20 i després tanca la font, de manera
    # que el job acaba sol. Amb 'random' quedaria corrent per sempre i
    # hauries de cancel·lar-lo a mà des de la UI.
    t_env.execute_sql("""
        CREATE TABLE origen_fals (
            id        INT,
            missatge  STRING
        ) WITH (
            'connector'          = 'datagen',
            'fields.id.kind'     = 'sequence',
            'fields.id.start'    = '1',
            'fields.id.end'      = '20',
            'fields.missatge.length' = '8'
        )
    """)

    # ── Sink JDBC ───────────────────────────────────────────────────
    # La PRIMARY KEY NOT ENFORCED activa el mode upsert: si rellances
    # el job, fa UPSERT en comptes de duplicar. Flink no valida la clau,
    # només l'utilitza per generar la sentència ON CONFLICT.
    #
    # 'sink.buffer-flush.interval' baix perquè durant el desenvolupament
    # vulguis veure les files a Postgres immediatament. En producció
    # es pujaria per rendiment.
    t_env.execute_sql(f"""
        CREATE TABLE smoke_sink (
            id        INT,
            missatge  STRING,
            PRIMARY KEY (id) NOT ENFORCED
        ) WITH (
            'connector'  = 'jdbc',
            'url'        = '{pg_url}',
            'table-name' = 'bronze.smoke_test',
            'username'   = '{pg_user}',
            'password'   = '{pg_password}',
            'sink.buffer-flush.interval'  = '1s',
            'sink.buffer-flush.max-rows'  = '10'
        )
    """)

    print(">>> Llançant prova de foc Flink -> PostgreSQL...")
    t_env.execute_sql(
        "INSERT INTO smoke_sink SELECT id, missatge FROM origen_fals"
    ).wait()
    print(">>> Job finalitzat. Verifica el recompte a Postgres.")


if __name__ == "__main__":
    main()
