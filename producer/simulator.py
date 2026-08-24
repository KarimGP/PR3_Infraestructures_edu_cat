"""
PRODUCTOR D'ESDEVENIMENTS
=========================

Simula el flux en temps real que arribaria a un sistema de gestio de
manteniment: incidencies que es reporten, canvis d'estat, visites
d'operaris i telemetria de sensors.

COHERENCIA AMB LES DADES EXISTENTS
----------------------------------
Els esdeveniments es refereixen a centres REALS carregats a ops.centres.
Si inventessim codis a l'atzar, les taules bronze no lligarien amb res i
tot el llinatge quedaria trencat: no podries fer un JOIN entre el que
arriba pel stream i el catalog de centres.

CLAU DEL MISSATGE
-----------------
La clau es el codi_centre. Kafka garanteix l'ordre DINS d'una particio,
i la clau determina a quina particio va el missatge. Aixi, tots els
esdeveniments d'un mateix centre arriben ordenats. Sense aixo, un
CANVI_ESTAT podria processar-se abans de la INCIDENCIA_OBERTA
corresponent.

MODES
-----
  --mode realtime   emet en temps real, per veure el pipeline en directe
  --mode burst      historia accelerada, per generar volum rapidament

Us:
    python producer/simulator.py --mode realtime --rate 5
    python producer/simulator.py --mode burst --events 20000
"""

from __future__ import annotations

import argparse
import json
import os
import signal
import sys
import time
import uuid
from datetime import datetime, timedelta, timezone

import numpy as np
import psycopg2
from confluent_kafka import Producer

TOPIC = os.getenv("KAFKA_TOPIC", "infraedu.events")
# Des de Windows sempre localhost:9092. El kafka:29092 nomes val des de
# dins de la xarxa Docker (Flink). Aixo ja ho vam patir el dia 1.
BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_EXTERN", "localhost:9092")

# Proporcions dels tipus d'esdeveniment. Les lectures de sensor dominen
# perque son telemetria periodica: son les que justifiquen la finestra
# d'agregacio de Flink, ja que amb un consumidor simple serien incomodes.
TIPUS_EVENT = ["LECTURA_SENSOR", "INCIDENCIA_OBERTA", "CANVI_ESTAT", "ACTUACIO_REGISTRADA"]
TIPUS_P = [0.72, 0.11, 0.10, 0.07]

ESTATS = ["ASSIGNADA", "EN_CURS", "RESOLTA_PROVISIONAL", "RESOLTA", "TANCADA"]
MAGNITUDS = ["TEMPERATURA", "CONSUM_ELECTRIC", "CONSUM_AIGUA", "CO2"]
CANALS = ["TELEFON", "WEB", "SENSOR", "INSPECCIO"]

aturar = False


def gestiona_senyal(signum, frame):
    """Ctrl+C atura net, fent flush del que quedi al buffer."""
    global aturar
    aturar = True
    print("\nAturant... (flush del buffer pendent)")


def carrega_centres(limit=600):
    """Llegeix centres reals. Sense aixo, els events no lligarien amb res."""
    conn = psycopg2.connect(
        host="localhost", port=5432,
        dbname=os.getenv("POSTGRES_DB", "infraedu"),
        user=os.getenv("POSTGRES_USER", "pr3"),
        password=os.getenv("POSTGRES_PASSWORD", "pr3_local_dev"),
    )
    with conn, conn.cursor() as cur:
        cur.execute("""
            SELECT c.codi_centre, c.num_alumnes, c.any_construccio
            FROM ops.centres c
            WHERE c.num_alumnes IS NOT NULL
            ORDER BY c.codi_centre
            LIMIT %s
        """, (limit,))
        centres = cur.fetchall()
    conn.close()
    if not centres:
        sys.exit("Cap centre amb atributs. Executa scripts/generate_synthetic.py primer.")
    return centres


def carrega_tipus():
    conn = psycopg2.connect(
        host="localhost", port=5432,
        dbname=os.getenv("POSTGRES_DB", "infraedu"),
        user=os.getenv("POSTGRES_USER", "pr3"),
        password=os.getenv("POSTGRES_PASSWORD", "pr3_local_dev"),
    )
    with conn, conn.cursor() as cur:
        cur.execute("SELECT codi_tipus, familia, pes_relatiu FROM ops.tipus_incidencia ORDER BY codi_tipus")
        tipus = cur.fetchall()
    conn.close()
    return tipus


def construeix_event(rng, centres, tipus_codis, tipus_pesos, moment):
    """Genera un esdeveniment. Retorna (clau, diccionari)."""
    codi_centre, alumnes, any_c = centres[rng.integers(0, len(centres))]
    tipus_event = str(rng.choice(TIPUS_EVENT, p=TIPUS_P))

    event = {
        "event_id": str(uuid.UUID(bytes=rng.bytes(16))),
        "event_type": tipus_event,
        # Flink espera ISO-8601 amb mil·lisegons i sufix Z. El isoformat()
        # de Python dona microsegons i offset +00:00, que el parser JSON de
        # Flink no accepta per a TIMESTAMP_LTZ(3).
        "event_ts": moment.isoformat(timespec="milliseconds").replace("+00:00", "Z"),
        "codi_centre": codi_centre,
    }

    if tipus_event == "LECTURA_SENSOR":
        magnitud = str(rng.choice(MAGNITUDS))
        # Valors plausibles segons la magnitud. Un consum electric i una
        # temperatura no viuen a la mateixa escala.
        valor = {
            "TEMPERATURA": float(rng.normal(21, 3.5)),
            "CONSUM_ELECTRIC": float(max(0, rng.normal(alumnes * 0.09, alumnes * 0.03))),
            "CONSUM_AIGUA": float(max(0, rng.normal(alumnes * 0.014, alumnes * 0.005))),
            "CO2": float(max(350, rng.normal(720, 210))),
        }[magnitud]
        event["payload"] = {
            "magnitud": magnitud,
            "valor": round(valor, 3),
            "unitat": {"TEMPERATURA": "C", "CONSUM_ELECTRIC": "kWh",
                       "CONSUM_AIGUA": "m3", "CO2": "ppm"}[magnitud],
            "sensor_id": f"SNS-{codi_centre}-{int(rng.integers(1, 9)):02d}",
        }

    elif tipus_event == "INCIDENCIA_OBERTA":
        i = int(rng.choice(len(tipus_codis), p=tipus_pesos))
        event["payload"] = {
            "codi_tipus": tipus_codis[i],
            "canal": str(rng.choice(CANALS, p=[0.42, 0.33, 0.10, 0.15])),
            "requereix_seguretat": bool(rng.random() < 0.18),
            "interromp_activitat": bool(rng.random() < 0.22),
        }

    elif tipus_event == "CANVI_ESTAT":
        event["payload"] = {
            "referencia": str(uuid.UUID(bytes=rng.bytes(16))),
            "estat_nou": str(rng.choice(ESTATS)),
        }

    else:  # ACTUACIO_REGISTRADA
        event["payload"] = {
            "referencia": str(uuid.UUID(bytes=rng.bytes(16))),
            "hores": round(float(rng.uniform(0.5, 8)), 2),
            "cost": round(float(rng.lognormal(5.2, 0.6)), 2),
        }

    # La clau determina la particio: mateix centre -> mateixa particio
    # -> ordre garantit.
    return codi_centre, event


def informe_lliurament(err, msg):
    if err is not None:
        print(f"  ERROR en lliurar: {err}")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--mode", choices=["realtime", "burst"], default="realtime")
    ap.add_argument("--rate", type=float, default=5.0,
                    help="events/segon en mode realtime")
    ap.add_argument("--events", type=int, default=10_000,
                    help="total d'events en mode burst")
    ap.add_argument("--dies", type=int, default=3,
                    help="dies d'historia a comprimir en mode burst")
    ap.add_argument("--llavor", type=int, default=7)
    args = ap.parse_args()

    signal.signal(signal.SIGINT, gestiona_senyal)
    rng = np.random.default_rng(args.llavor)

    print(f"Connectant a Kafka: {BOOTSTRAP}")
    centres = carrega_centres()
    tipus = carrega_tipus()
    tipus_codis = [t[0] for t in tipus]
    tipus_pesos = np.array([float(t[2]) for t in tipus])
    tipus_pesos = tipus_pesos / tipus_pesos.sum()
    print(f"Centres carregats: {len(centres)}")
    print(f"Tipus carregats  : {len(tipus)}")
    print(f"Topic            : {TOPIC}\n")

    producer = Producer({
        "bootstrap.servers": BOOTSTRAP,
        "client.id": "pr3-simulator",
        # Espera confirmacio de totes les repliques abans de considerar
        # el missatge lliurat. Amb un sol broker es equivalent a acks=1,
        # pero deixa el codi correcte si algun dia hi ha mes brokers.
        "acks": "all",
        "linger.ms": 50,
        "compression.type": "snappy",
    })

    enviats = 0
    inici = time.time()

    if args.mode == "realtime":
        print(f"Mode TEMPS REAL a {args.rate} events/s. Ctrl+C per aturar.\n")
        interval = 1.0 / args.rate
        while not aturar:
            moment = datetime.now(timezone.utc)
            clau, event = construeix_event(rng, centres, tipus_codis, tipus_pesos, moment)
            producer.produce(TOPIC, key=clau.encode(),
                             value=json.dumps(event).encode(),
                             callback=informe_lliurament)
            producer.poll(0)
            enviats += 1
            if enviats % 25 == 0:
                print(f"  {enviats} events enviats ({event['event_type']})")
            time.sleep(interval)
    else:
        print(f"Mode BURST: {args.events} events repartits en {args.dies} dies.\n")
        # Repartim els events cap enrere des d'ara, per tenir historia
        # amb la qual alimentar les finestres d'agregacio de Flink.
        ara = datetime.now(timezone.utc)
        finestra = timedelta(days=args.dies)
        for i in range(args.events):
            if aturar:
                break
            desplacament = finestra * (1 - i / args.events)
            moment = ara - desplacament
            clau, event = construeix_event(rng, centres, tipus_codis, tipus_pesos, moment)
            producer.produce(TOPIC, key=clau.encode(),
                             value=json.dumps(event).encode(),
                             callback=informe_lliurament)
            enviats += 1
            if enviats % 2000 == 0:
                producer.poll(0)
                print(f"  {enviats}/{args.events} events...")

    print("\nFent flush del buffer...")
    pendents = producer.flush(30)
    durada = time.time() - inici
    print(f"Enviats  : {enviats}")
    print(f"Pendents : {pendents}")
    print(f"Durada   : {durada:.1f}s ({enviats/max(durada,0.001):.0f} ev/s)")


if __name__ == "__main__":
    main()
