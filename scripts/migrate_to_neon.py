"""
MIGRACIO DELS MARTS A NEON (PostgreSQL al nuvol)
=================================================

Copia les taules del model dimensional del Postgres local a una
instancia de Neon, perque el dashboard de Power BI publicat pugui
consultar-les sense dependre de la maquina de desenvolupament.

QUE ES MIGRA I QUE NO
---------------------
Nomes els MARTS i les prediccions: es l'unic que consumeix Power BI.
No es migra ni ops, ni bronze, ni staging. Motius:
  * Volum: bronze.raw_events te 35.000 files que no serveixen al
    dashboard, i el pla gratuit de Neon te 0,5 GB.
  * Seguretat: la capa operacional no ha de sortir de l'entorn.
  * Proposit: Neon aqui es una capa de publicacio, no una replica.

L'arquitectura real no canvia: el pipeline segueix corrent en local i
Neon nomes rep el resultat final.

Us:
    python scripts/migrate_to_neon.py
    python scripts/migrate_to_neon.py --nomes dim_centre
"""

from __future__ import annotations

import argparse
import io
import os
import sys

import psycopg2
from dotenv import load_dotenv

load_dotenv()

# Taules a migrar, en ordre de dependencia (dimensions primer).
TAULES = [
    ("dbt_marts", "dim_centre"),
    ("dbt_marts", "dim_temps"),
    ("dbt_marts", "dim_tipus_incidencia"),
    ("dbt_marts", "dim_empresa"),
    ("dbt_marts", "fct_incidencies"),
    ("dbt_marts", "fct_certificacions"),
    ("dbt_marts", "fct_events_streaming"),
    ("ml", "prediccions_incidencies"),
]


def conn_local():
    return psycopg2.connect(
        host="localhost", port=5432,
        dbname=os.getenv("POSTGRES_DB", "infraedu"),
        user=os.getenv("POSTGRES_USER", "pr3"),
        password=os.getenv("POSTGRES_PASSWORD", "pr3_local_dev"),
    )


def conn_neon():
    for var in ("NEON_HOST", "NEON_DB", "NEON_USER", "NEON_PASSWORD"):
        if not os.getenv(var):
            sys.exit(f"Falta {var} al fitxer .env")
    return psycopg2.connect(
        host=os.environ["NEON_HOST"],
        dbname=os.environ["NEON_DB"],
        user=os.environ["NEON_USER"],
        password=os.environ["NEON_PASSWORD"],
        sslmode="require",
    )


def ddl_de_taula(cur_local, esquema, taula):
    """Genera el CREATE TABLE llegint el catalog de PostgreSQL.

    No copiem constraints ni indexos: la base de dades de publicacio
    es de nomes lectura i les dades ja venen validades pels 88 tests
    de dbt. Afegir-hi integritat referencial seria redundant.
    """
    cur_local.execute("""
        SELECT column_name, data_type, character_maximum_length,
               numeric_precision, numeric_scale
        FROM information_schema.columns
        WHERE table_schema = %s AND table_name = %s
        ORDER BY ordinal_position
    """, (esquema, taula))

    columnes = []
    for nom, tipus, maxlen, prec, escala in cur_local.fetchall():
        if tipus == "character varying" and maxlen:
            t = f"VARCHAR({maxlen})"
        elif tipus == "numeric" and prec:
            t = f"NUMERIC({prec},{escala or 0})"
        elif tipus == "timestamp with time zone":
            t = "TIMESTAMPTZ"
        elif tipus == "timestamp without time zone":
            t = "TIMESTAMP"
        elif tipus == "double precision":
            t = "DOUBLE PRECISION"
        else:
            t = tipus.upper()
        columnes.append(f'    "{nom}" {t}')

    return f'CREATE TABLE {esquema}.{taula} (\n' + ",\n".join(columnes) + "\n)"


def migra(cur_local, cur_neon, esquema, taula):
    print(f"\n{taula}")

    cur_local.execute(f"SELECT count(*) FROM {esquema}.{taula}")
    total = cur_local.fetchone()[0]
    print(f"  origen: {total:,} files")

    cur_neon.execute(f"DROP TABLE IF EXISTS {esquema}.{taula} CASCADE")
    cur_neon.execute(ddl_de_taula(cur_local, esquema, taula))

    # COPY a traves d'un buffer en memoria: molt mes rapid que
    # INSERT fila a fila, sobretot amb latencia de xarxa.
    buffer = io.StringIO()
    cur_local.copy_expert(
        f"COPY {esquema}.{taula} TO STDOUT WITH CSV", buffer)
    buffer.seek(0)
    cur_neon.copy_expert(
        f"COPY {esquema}.{taula} FROM STDIN WITH CSV", buffer)

    cur_neon.execute(f"SELECT count(*) FROM {esquema}.{taula}")
    print(f"  desti : {cur_neon.fetchone()[0]:,} files")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--nomes", default=None, help="Migra nomes aquesta taula")
    args = ap.parse_args()

    taules = ([t for t in TAULES if t[1] == args.nomes]
              if args.nomes else TAULES)
    if not taules:
        sys.exit(f"Taula desconeguda: {args.nomes}")

    local, neon = conn_local(), conn_neon()
    print(f"Origen : localhost/infraedu")
    print(f"Desti  : {os.environ['NEON_HOST']}")

    try:
        with local, neon, local.cursor() as cl, neon.cursor() as cn:
            cn.execute("CREATE SCHEMA IF NOT EXISTS dbt_marts")
            cn.execute("CREATE SCHEMA IF NOT EXISTS ml")
            for esquema, taula in taules:
                migra(cl, cn, esquema, taula)
        print("\nMigracio completada.")
    finally:
        local.close()
        neon.close()


if __name__ == "__main__":
    main()