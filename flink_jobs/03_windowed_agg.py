"""FLINK - Finestres d'agregacio -> bronze.agg_events_5min

Aquest job es el que justifica tenir Flink al projecte. El job 02 es un
pas-a-traves que faria igual un consumidor Python amb un INSERT. Aqui hi
ha COMPUTACIO AMB ESTAT: Flink mante en memoria els esdeveniments de
cada finestra oberta, sap quan tancar-la i emet el resultat agregat.

TUMBLE: finestres fixes de 5 minuts, no solapades. Cada esdeveniment cau
en exactament una finestra.

EVENT TIME, NO PROCESSING TIME: la finestra s'assigna segons event_ts
(quan va PASSAR la cosa), no segons quan Flink la processa. Aixi, si
reprocesses el topic dema, els resultats son IDENTICS.

El WATERMARK de 10s diu a Flink quan pot donar una finestra per tancada.
"""

import os
import sys

from pyflink.table import EnvironmentSettings, TableEnvironment


def main() -> None:
    t_env = TableEnvironment.create(EnvironmentSettings.in_streaming_mode())
    cfg = t_env.get_config()
    cfg.set("parallelism.default", "1")
    cfg.set("pipeline.name", "windowed-agg-5min")

    try:
        pg_url = os.environ["PG_URL"]
        pg_user = os.environ["PG_USER"]
        pg_password = os.environ["PG_PASSWORD"]
        kafka = os.environ["KAFKA_BOOTSTRAP"]
    except KeyError as e:
        sys.exit(f"Falta la variable d'entorn {e}.")

    # Grup de consumidors DIFERENT del job 02: els dos llegeixen el mateix
    # topic independentment. Amb el mateix group.id es repartirien les
    # particions i cap dels dos veuria tots els esdeveniments.
    t_env.execute_sql(f"""
        CREATE TABLE events_kafka (
            event_id     STRING,
            event_type   STRING,
            event_ts     TIMESTAMP_LTZ(3),
            codi_centre  STRING,
            payload      STRING,
            WATERMARK FOR event_ts AS event_ts - INTERVAL '10' SECOND
        ) WITH (
            'connector' = 'kafka',
            'topic' = 'infraedu.events',
            'properties.bootstrap.servers' = '{kafka}',
            'properties.group.id' = 'flink-window-agg',
            'scan.startup.mode' = 'earliest-offset',
            'format' = 'json',
            'json.timestamp-format.standard' = 'ISO-8601',
            'json.ignore-parse-errors' = 'false'
        )
    """)

    # La clau primaria coincideix amb el GROUP BY: el sink fa UPSERT i
    # rellancar el job no duplica finestres.
    t_env.execute_sql(f"""
        CREATE TABLE agg_sink (
            finestra_inici  TIMESTAMP(3),
            finestra_fi     TIMESTAMP(3),
            codi_centre     STRING,
            event_type      STRING,
            num_events      INT,
            valor_mitja     DECIMAL(12,4),
            valor_max       DECIMAL(12,4),
            PRIMARY KEY (finestra_inici, codi_centre, event_type) NOT ENFORCED
        ) WITH (
            'connector'  = 'jdbc',
            'url'        = '{pg_url}?stringtype=unspecified',
            'table-name' = 'bronze.agg_events_5min',
            'username'   = '{pg_user}',
            'password'   = '{pg_password}',
            'sink.buffer-flush.interval' = '2s',
            'sink.buffer-flush.max-rows' = '200',
            'sink.max-retries' = '3'
        )
    """)

    print(">>> Iniciant agregacio per finestres de 5 minuts")
    print(">>> Les finestres no s'emeten fins que el watermark les tanca.")

    # JSON_VALUE extreu el valor numeric del sensor. Nomes les
    # LECTURA_SENSOR en tenen; la resta donen NULL i les funcions
    # d'agregacio els ignoren automaticament.
    t_env.execute_sql("""
        INSERT INTO agg_sink
        SELECT
            CAST(window_start AS TIMESTAMP(3)),
            CAST(window_end   AS TIMESTAMP(3)),
            codi_centre,
            event_type,
            CAST(COUNT(*) AS INT),
            CAST(AVG(CAST(JSON_VALUE(payload, '$.valor') AS DOUBLE)) AS DECIMAL(12,4)),
            CAST(MAX(CAST(JSON_VALUE(payload, '$.valor') AS DOUBLE)) AS DECIMAL(12,4))
        FROM TABLE(
            TUMBLE(TABLE events_kafka, DESCRIPTOR(event_ts), INTERVAL '5' MINUTES)
        )
        GROUP BY window_start, window_end, codi_centre, event_type
    """)


if __name__ == "__main__":
    main()
