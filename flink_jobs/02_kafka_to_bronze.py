"""
FLINK · Kafka -> bronze.raw_events
==================================

Primer job real del pipeline: consumeix el topic infraedu.events,
desserialitza el JSON i aterra cada esdeveniment a la capa Bronze de
PostgreSQL sense transformar-lo.

PER QUE BRONZE NO TRANSFORMA
----------------------------
El payload es guarda sencer com a JSONB. Si dema el productor afegeix
un camp nou, no perdem informacio ni cal tocar l'esquema. Les
transformacions van a dbt, on queden documentades i testades. Barrejar
transformacio amb ingesta es el error classic que fa que despres no
puguis reprocessar res.

TEMPS D'ESDEVENIMENT I WATERMARK
--------------------------------
event_ts es el moment en que va PASSAR la cosa, no quan Flink la
processa. Fem servir temps d'esdeveniment (event time) i no temps de
proces (processing time) perque:

  * els esdeveniments poden arribar desordenats (particions diferents,
    reintents de xarxa, productors amb retard);
  * si reprocesses el topic dema, els resultats han de ser IDENTICS.
    Amb processing time, cada reproces donaria numeros diferents.

El WATERMARK de 10 segons li diu a Flink: "quan hagis vist un event de
les 10:00:30, dona per tancat tot el que sigui anterior a les 10:00:20".
Es el compromis entre esperar els que arriben tard i no bloquejar-se.
Aqui encara no fem finestres, pero definir-ho ara deixa el terreny
preparat per al job seguent.

METADADES DE PROCEDENCIA
------------------------
Guardem topic, particio i offset de Kafka. Son imprescindibles per
depurar ("d'on ha sortit aquesta fila?") i per demostrar llinatge.

Us:
    ./scripts/run_flink_job.sh 02_kafka_to_bronze.py
"""

import os
import sys

from pyflink.table import EnvironmentSettings, TableEnvironment


def main() -> None:
    t_env = TableEnvironment.create(EnvironmentSettings.in_streaming_mode())
    cfg = t_env.get_config()
    cfg.set("parallelism.default", "3")          # una tasca per particio
    cfg.set("pipeline.name", "kafka-to-bronze")

    try:
        pg_url = os.environ["PG_URL"]
        pg_user = os.environ["PG_USER"]
        pg_password = os.environ["PG_PASSWORD"]
        kafka = os.environ["KAFKA_BOOTSTRAP"]
    except KeyError as e:
        sys.exit(f"Falta la variable d'entorn {e}.")

    # ── Font: topic de Kafka ─────────────────────────────────────────
    # Les metadades (topic, particio, offset) s'obtenen amb columnes
    # METADATA VIRTUAL: no son al JSON, les aporta el connector.
    t_env.execute_sql(f"""
        CREATE TABLE events_kafka (
            event_id     STRING,
            event_type   STRING,
            event_ts     TIMESTAMP_LTZ(3),
            codi_centre  STRING,
            payload      STRING,
            topic_meta     STRING   METADATA FROM 'topic'     VIRTUAL,
            particio_meta  INT      METADATA FROM 'partition' VIRTUAL,
            offset_meta    BIGINT   METADATA FROM 'offset'    VIRTUAL,
            WATERMARK FOR event_ts AS event_ts - INTERVAL '10' SECOND
        ) WITH (
            'connector' = 'kafka',
            'topic' = 'infraedu.events',
            'properties.bootstrap.servers' = '{kafka}',
            'properties.group.id' = 'flink-bronze-loader',
            'scan.startup.mode' = 'earliest-offset',
            'format' = 'json',
            'json.timestamp-format.standard' = 'ISO-8601',
            'json.ignore-parse-errors' = 'false'
        )
    """)

    # ── Sink: bronze.raw_events ──────────────────────────────────────
    # PRIMARY KEY NOT ENFORCED activa el mode upsert: si rellances el
    # job des de l'inici del topic, fa UPSERT en lloc de duplicar.
    # Flink no valida la clau, nomes la fa servir per generar
    # l'ON CONFLICT de PostgreSQL.
    #
    # buffer-flush baix perque durant el desenvolupament vulguis veure
    # les files aparèixer de seguida. En produccio es pujaria.
    t_env.execute_sql(f"""
        CREATE TABLE bronze_sink (
            event_id        STRING,
            event_type      STRING,
            event_ts        TIMESTAMP(3),
            codi_centre     STRING,
            payload         STRING,
            kafka_topic     STRING,
            kafka_partition INT,
            kafka_offset    BIGINT,
            PRIMARY KEY (event_id) NOT ENFORCED
        ) WITH (
            'connector'  = 'jdbc',
            'url'        = '{pg_url}?stringtype=unspecified',
            'table-name' = 'bronze.raw_events',
            'username'   = '{pg_user}',
            'password'   = '{pg_password}',
            'sink.buffer-flush.interval' = '2s',
            'sink.buffer-flush.max-rows' = '500',
            'sink.max-retries' = '3'
        )
    """)

    print(">>> Iniciant ingesta Kafka -> bronze.raw_events")
    print(">>> El job queda corrent indefinidament. Per aturar-lo:")
    print(">>>   docker exec pr3-flink-jm ./bin/flink list")
    print(">>>   docker exec pr3-flink-jm ./bin/flink cancel <JobID>")

    t_env.execute_sql("""
        INSERT INTO bronze_sink
        SELECT event_id,
                event_type,
                -- El connector JDBC de Flink no suporta TIMESTAMP_LTZ.
                -- Mantenim LTZ a la font (cal per al watermark i les
                -- finestres) i convertim a TIMESTAMP nomes al sink.
                CAST(event_ts AS TIMESTAMP(3)),
                codi_centre,
                payload,
                topic_meta, particio_meta, offset_meta
        FROM events_kafka
    """)


if __name__ == "__main__":
    main()
