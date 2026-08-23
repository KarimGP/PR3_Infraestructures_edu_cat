"""Exploracio del Directori de centres docents (Transparencia Catalunya).
No carrega res: nomes inspecciona l'estructura del dataset abans
d'escriure el codi de carrega.
Dataset kvmv-ahh4, API Socrata (la mateixa familia que al PR1).
"""
import json
import sys
from collections import Counter

import requests

DATASET_ID = "kvmv-ahh4"
BASE_URL = f"https://analisi.transparenciacatalunya.cat/resource/{DATASET_ID}.json"


def sep(titol):
    print(f"\n{'=' * 70}\n{titol}\n{'=' * 70}")


def main():
    sep("1. MOSTRA - primer registre")
    try:
        r = requests.get(BASE_URL, params={"$limit": 5}, timeout=30)
        r.raise_for_status()
    except requests.RequestException as e:
        sys.exit(f"Error accedint a l'API: {e}")
    mostra = r.json()
    if not mostra:
        sys.exit("L'API no ha retornat cap registre.")
    print(json.dumps(mostra[0], indent=2, ensure_ascii=False))

    sep("2. COLUMNES detectades")
    columnes = set()
    for reg in mostra:
        columnes.update(reg.keys())
    for c in sorted(columnes):
        print(f"  - {c}")
    print(f"\n  Total: {len(columnes)} columnes")

    sep("3. VOLUM total")
    r = requests.get(BASE_URL, params={"$select": "count(*) as total"}, timeout=30)
    r.raise_for_status()
    print(f"  {int(r.json()[0]['total']):,} registres")

    sep("4. CURSOS disponibles (duplicats per centre)")
    camp_curs = next((c for c in columnes if "curs" in c.lower()), None)
    if camp_curs:
        r = requests.get(BASE_URL, params={
            "$select": f"{camp_curs}, count(*) as n",
            "$group": camp_curs,
            "$order": f"{camp_curs} DESC"}, timeout=30)
        r.raise_for_status()
        for fila in r.json():
            print(f"  {fila[camp_curs]}: {int(fila['n']):,}")
        print(f"\n  -> Camp de curs: '{camp_curs}'")
    else:
        print("  No s'ha detectat cap camp de curs.")

    sep("5. COBERTURA per columna (mostra de 1000)")
    r = requests.get(BASE_URL, params={"$limit": 1000}, timeout=60)
    r.raise_for_status()
    m1000 = r.json()
    presents = Counter()
    for reg in m1000:
        for k, v in reg.items():
            if v not in (None, "", []):
                presents[k] += 1
    for col in sorted(set(presents) | columnes):
        pct = 100 * presents.get(col, 0) / len(m1000)
        marca = "OK" if pct > 95 else ("~~" if pct > 50 else "!!")
        print(f"  {marca}  {col:<45} {pct:5.1f}%")
    print("\n  OK = gairebe sempre  ~~ = parcial  !! = molt buida")

    sep("FI")


if __name__ == "__main__":
    main()
