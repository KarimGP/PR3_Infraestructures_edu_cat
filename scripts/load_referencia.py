"""Carrega de dades de referencia reals a ops.

Font: Directori de centres docents anual (Transparencia Catalunya, kvmv-ahh4).
Es filtra pel curs mes recent perque el dataset acumula un registre per
centre I curs; sense filtrar tindriem codis repetits.

Comarques i municipis es deriven del mateix dataset: ja hi venen com a
camps denormalitzats. Evitem una segona font i garantim coherencia
referencial per construccio.

Ordre obligatori per les claus foranes: comarques -> municipis -> centres.

Us:  python scripts/load_referencia.py
"""
import os
import sys

import psycopg2
import requests
from psycopg2.extras import execute_values

BASE = "https://analisi.transparenciacatalunya.cat/resource/kvmv-ahh4.json"
CURS = "2025/2026"

# La provincia no ve al dataset pero l'esquema la demana.
# Els dos primers digits del codi INE la determinen.
PROVINCIES = {"08": "Barcelona", "17": "Girona", "25": "Lleida", "43": "Tarragona"}

# Mapatge de les banderes d'ensenyament de la font a les columnes nostres.
# La font marca amb un text curt si el centre imparteix l'ensenyament;
# si el camp no hi es, no l'imparteix.
BANDERES = {
    "einf1c": "te_infantil_1c",
    "einf2c": "te_infantil_2c",
    "epri":   "te_primaria",
    "eso":    "te_eso",
    "batx":   "te_batxillerat",
    "cfpm":   "te_fp_mitja",
    "cfps":   "te_fp_superior",
    "adults": "te_adults",
    "ee":     "te_especial",
}


def descarrega():
    """Baixa tots els centres del curs amb paginacio."""
    registres, offset, LIMIT = [], 0, 1000
    while True:
        r = requests.get(BASE, params={
            "$where": f"curs='{CURS}'",
            "$order": "codi_centre",
            "$limit": LIMIT,
            "$offset": offset,
        }, timeout=60)
        r.raise_for_status()
        lot = r.json()
        if not lot:
            break
        registres.extend(lot)
        offset += LIMIT
        print(f"  descarregats {len(registres)}...")
    return registres


def num(valor):
    """Converteix a float o None. Les coordenades poden venir buides."""
    try:
        return float(valor)
    except (TypeError, ValueError):
        return None


def main():
    print(f"Descarregant centres del curs {CURS}...")
    dades = descarrega()
    print(f"Total: {len(dades)} centres\n")

    # ── Derivem comarques ────────────────────────────────────────────
    comarques = {}
    for d in dades:
        codi = d["codi_comarca"].zfill(2)
        prov = PROVINCIES.get(d["codi_municipi_6"][:2])
        if prov is None:
            print(f"  AVIS: provincia desconeguda per {d['codi_municipi_6']}")
            continue
        comarques[codi] = (codi, d["nom_comarca"], prov)

    # ── Derivem municipis ────────────────────────────────────────────
    municipis = {}
    for d in dades:
        codi = d["codi_municipi_6"]
        municipis[codi] = (codi, d["nom_municipi"], d["codi_comarca"].zfill(2))

    # ── Centres ──────────────────────────────────────────────────────
    centres = []
    for d in dades:
        banderes = [bool(d.get(font)) for font in BANDERES]
        centres.append((
            d["codi_centre"],
            d["denominaci_completa"],
            d.get("codi_naturalesa"),
            d.get("nom_naturalesa"),
            d.get("codi_titularitat"),
            d.get("nom_titularitat"),
            d["codi_municipi_6"],
            d.get("adre_a"),
            d.get("codi_postal"),
            num(d.get("coordenades_geo_y")),   # latitud
            num(d.get("coordenades_geo_x")),   # longitud
            *banderes,
            CURS,
        ))

    print(f"Comarques : {len(comarques)}")
    print(f"Municipis : {len(municipis)}")
    print(f"Centres   : {len(centres)}\n")

    # ── Carrega transaccional ────────────────────────────────────────
    # Tot o res: si falla qualsevol insercio, no queda res a mitges.
    conn = psycopg2.connect(
        host="localhost", port=5432,
        dbname=os.getenv("POSTGRES_DB", "infraedu"),
        user=os.getenv("POSTGRES_USER", "pr3"),
        password=os.getenv("POSTGRES_PASSWORD", "pr3_local_dev"),
    )
    try:
        with conn, conn.cursor() as cur:
            execute_values(cur,
                "INSERT INTO ops.comarques (codi_comarca, nom, provincia) VALUES %s "
                "ON CONFLICT (codi_comarca) DO NOTHING",
                list(comarques.values()))
            print(f"  comarques inserides: {cur.rowcount}")

            execute_values(cur,
                "INSERT INTO ops.municipis (codi_ine, nom, codi_comarca) VALUES %s "
                "ON CONFLICT (codi_ine) DO NOTHING",
                list(municipis.values()))
            print(f"  municipis inserits : {cur.rowcount}")

            cols = ("codi_centre, denominacio, codi_naturalesa, nom_naturalesa, "
                    "codi_titularitat, nom_titularitat, codi_ine, adreca, codi_postal, "
                    "latitud, longitud, "
                    + ", ".join(BANDERES.values()) + ", curs_font")
            execute_values(cur,
                f"INSERT INTO ops.centres ({cols}) VALUES %s "
                "ON CONFLICT (codi_centre) DO NOTHING",
                centres)
            print(f"  centres inserits   : {cur.rowcount}")
    finally:
        conn.close()

    print("\nCarrega completada.")


if __name__ == "__main__":
    main()
