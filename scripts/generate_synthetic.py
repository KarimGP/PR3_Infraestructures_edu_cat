"""
GENERADOR DE DADES SINTETIQUES
==============================

Genera la capa transaccional del sistema: incidencies de manteniment,
actuacions, projectes d'inversio i certificacions, sobre els centres
educatius reals ja carregats a ops.centres.

QUE ES REAL I QUE ES SIMULAT
----------------------------
REAL  (font: Transparencia Catalunya, kvmv-ahh4):
      centres, denominacio, ubicacio, coordenades, ensenyaments,
      titularitat, municipis i comarques.
SIMULAT (generat aqui):
      num_alumnes, superficie, any_construccio, estat_conservacio,
      empreses, contractes, incidencies, actuacions, projectes,
      certificacions.

Aquesta distincio ha de quedar explicita al README. Presentar dades
inventades com si fossin reals invalidaria tot el projecte.

PARAMETRES OPERATIUS
--------------------
Els patrons no son arbitraris: venen de l'experiencia en gestio de
manteniment d'infraestructures educatives.

  * Volum: ~28 incidencies per centre i any, proporcional a la mida
    del centre pero amb excepcions (hi ha centres que en generen molt
    mes del que els tocaria per alumnes).
  * Families: fontaneria i climatitzacio majoritaries, despres
    paleteria i electricitat, fusteria, i estructura residual.
  * Estacionalitat: nomes la climatitzacio en te. Els pics son al
    novembre i al maig, quan s'arrenquen els sistemes despres de mesos
    aturats: les averies surten en posar en marxa l'equip, no quan fa
    mes fred o mes calor. La resta de families son planes tot l'any.
  * Calendari escolar: julio i agost gairebe sense activitat.
  * Antiguitat: efecte DELIBERADAMENT FEBLE. Un centre dels anys 60
    genera un ~30% mes que un del 2015, no el doble. Hi ha centres nous
    amb forca incidencies. Aixo fa que l'antiguitat sigui un predictor
    debil al model d'ML, que es mes realista i mes interessant que un
    dataset on una sola variable ho explica tot.
  * SLA: 24h si afecta la seguretat o interromp l'activitat docent
    (encara que la solucio sigui provisional); 120h la resta.
  * El 50% de les incidencies urgents resoltes provisionalment
    necessiten despres una intervencio definitiva.

REPRODUCIBILITAT
----------------
Tot el generador va amb llavor fixa. Dues execucions produeixen
exactament les mateixes dades. Sense aixo, els resultats del model del
dia 13 no serien comparables entre execucions.

Us:
    python scripts/generate_synthetic.py
    python scripts/generate_synthetic.py --centres 600 --incidencies 50000
"""

from __future__ import annotations

import argparse
import os
import uuid
from datetime import date, datetime, timedelta, timezone

import numpy as np
import psycopg2
from psycopg2.extras import execute_values

# ─────────────────────────────────────────────────────────────────────
# Configuracio
# ─────────────────────────────────────────────────────────────────────

LLAVOR = 42
COMARCA_AMBIT = "Barcelonès"
DATA_INICI = date(2023, 9, 1)
DATA_FI = date(2026, 8, 31)

# Multiplicadors mensuals. Index 0 = gener.
# La climatitzacio te pics al novembre i al maig (arrencada de sistemes).
EST_CLIMA = np.array([1.30, 1.00, 0.80, 0.70, 2.00, 1.20,
                      0.15, 0.10, 0.80, 1.00, 2.20, 1.40])
# La resta de families son planes, amb la vall d'estiu del calendari escolar.
EST_ALTRES = np.array([1.00, 1.00, 1.00, 0.90, 1.00, 1.00,
                       0.15, 0.10, 1.10, 1.00, 1.00, 0.85])

CANALS = ["TELEFON", "WEB", "SENSOR", "INSPECCIO"]
CANALS_P = [0.42, 0.33, 0.10, 0.15]

EMPRESES = [
    ("B60123456", "Manteniments Integrals Bcn SL", 1),
    ("B61234567", "Serveis Tecnics Litoral SA", 2),
    ("B62345678", "Conservacio i Obres Besos SL", 3),
]

TIPOLOGIES_INVERSIO = [
    ("REFORMA", 0.32, 180_000, 90_000),
    ("EFICIENCIA_ENERGETICA", 0.24, 240_000, 110_000),
    ("ACCESSIBILITAT", 0.16, 95_000, 40_000),
    ("AMPLIACIO", 0.14, 620_000, 280_000),
    ("RETIRADA_AMIANT", 0.09, 150_000, 70_000),
    ("NOVA_CONSTRUCCIO", 0.05, 2_800_000, 900_000),
]


def connecta():
    return psycopg2.connect(
        host="localhost",
        port=int(os.getenv("PGPORT", "5432")),
        dbname=os.getenv("POSTGRES_DB", "infraedu"),
        user=os.getenv("POSTGRES_USER", "pr3"),
        password=os.getenv("POSTGRES_PASSWORD", "pr3_local_dev"),
    )


# ─────────────────────────────────────────────────────────────────────
# 1. Atributs sintetics dels centres
# ─────────────────────────────────────────────────────────────────────

def atributs_centre(rng, c):
    """Deriva mida i antiguitat a partir dels ensenyaments reals.

    Un institut amb ESO, batxillerat i FP no te la mateixa mida que una
    llar d'infants. Fer-ho dependre dels ensenyaments (que si que son
    reals) fa que la simulacio sigui coherent amb la font.
    """
    te_inf1, te_inf2, te_pri, te_eso, te_batx, te_fpm, te_fps, te_ad, te_ee = c[1:10]

    if te_ee:
        base = rng.integers(40, 130)
    elif te_ad:
        base = rng.integers(90, 420)
    elif te_eso or te_batx or te_fpm or te_fps:
        base = rng.integers(380, 1050)          # institut
    elif te_pri:
        base = rng.integers(150, 520)           # escola
    elif te_inf1 or te_inf2:
        base = rng.integers(35, 110)            # llar d'infants
    else:
        base = rng.integers(60, 250)

    alumnes = int(base)
    # Entre 8 i 14 m2 per alumne, mes una part fixa d'espais comuns.
    superficie = round(float(alumnes * rng.uniform(8, 14) + rng.uniform(250, 900)), 2)

    # Distribucio d'antiguitat del parc escolar catala: gran expansio
    # als 60-80, onada als 90-2000, i construccio recent minoritaria.
    tram = rng.choice([0, 1, 2, 3], p=[0.30, 0.28, 0.27, 0.15])
    any_c = int(rng.integers(*[(1955, 1980), (1980, 1995),
                               (1995, 2010), (2010, 2023)][tram]))

    # L'estat de conservacio depen de l'antiguitat pero amb molt de
    # soroll: hi ha centres antics ben mantinguts i nous amb problemes.
    edat = 2026 - any_c
    estat = int(np.clip(92 - edat * 0.42 + rng.normal(0, 12), 5, 100))

    return alumnes, superficie, any_c, estat


# ─────────────────────────────────────────────────────────────────────
# 2. Mostreig de dates amb estacionalitat
# ─────────────────────────────────────────────────────────────────────

def construeix_calendari():
    """Retorna els dies del periode i els pesos de cada dia, per a
    climatitzacio i per a la resta de families.

    El pes combina tres factors: el multiplicador mensual, una caiguda
    forta els caps de setmana (els centres estan tancats i les
    incidencies es detecten dilluns) i el calendari escolar.
    """
    dies = []
    d = DATA_INICI
    while d <= DATA_FI:
        dies.append(d)
        d += timedelta(days=1)

    w_clima, w_altres = [], []
    for d in dies:
        # Cap de setmana: molt poca activitat de deteccio.
        factor_dia = 0.12 if d.weekday() >= 5 else 1.0
        w_clima.append(EST_CLIMA[d.month - 1] * factor_dia)
        w_altres.append(EST_ALTRES[d.month - 1] * factor_dia)

    w_clima = np.array(w_clima)
    w_altres = np.array(w_altres)
    return dies, w_clima / w_clima.sum(), w_altres / w_altres.sum()


# ─────────────────────────────────────────────────────────────────────
# 3. Generacio principal
# ─────────────────────────────────────────────────────────────────────

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--centres", type=int, default=600)
    ap.add_argument("--incidencies", type=int, default=50_000)
    ap.add_argument("--llavor", type=int, default=LLAVOR)
    args = ap.parse_args()

    rng = np.random.default_rng(args.llavor)
    conn = connecta()
    cur = conn.cursor()

    # ── Selecciona l'ambit ───────────────────────────────────────────
    cur.execute("""
        SELECT c.codi_centre, c.te_infantil_1c, c.te_infantil_2c, c.te_primaria,
               c.te_eso, c.te_batxillerat, c.te_fp_mitja, c.te_fp_superior,
               c.te_adults, c.te_especial, c.codi_ine
        FROM ops.centres c
        JOIN ops.municipis m ON m.codi_ine = c.codi_ine
        JOIN ops.comarques co ON co.codi_comarca = m.codi_comarca
        WHERE co.nom = %s
        ORDER BY c.codi_centre
    """, (COMARCA_AMBIT,))
    tots = cur.fetchall()
    if not tots:
        raise SystemExit(f"Cap centre a {COMARCA_AMBIT}. Has carregat load_referencia.py?")
    print(f"Centres a {COMARCA_AMBIT}: {len(tots)}")

    # Mostra determinista: ordenats per codi i triats amb llavor fixa.
    idx = rng.choice(len(tots), size=min(args.centres, len(tots)), replace=False)
    centres = [tots[i] for i in sorted(idx)]
    print(f"Seleccionats            : {len(centres)}\n")

    # ── Atributs sintetics ───────────────────────────────────────────
    atributs, info = [], {}
    for c in centres:
        alumnes, sup, any_c, estat = atributs_centre(rng, c)
        atributs.append((alumnes, sup, any_c, estat, c[0]))
        info[c[0]] = {"alumnes": alumnes, "any": any_c, "ine": c[10]}

    cur.executemany("""
        UPDATE ops.centres SET num_alumnes=%s, superficie_m2=%s,
               any_construccio=%s, estat_conservacio=%s
        WHERE codi_centre=%s
    """, atributs)
    print(f"Atributs actualitzats   : {len(atributs)} centres")

    # ── Empreses, lots i contractes ──────────────────────────────────
    cur.execute("TRUNCATE ops.empreses, ops.contractes, ops.lot_cobertura CASCADE")
    execute_values(cur,
        "INSERT INTO ops.empreses (nif, nom, tipus) VALUES %s",
        [(nif, nom, "MANTENIMENT") for nif, nom, _ in EMPRESES])

    # Reparteix els municipis de l'ambit entre els tres lots.
    municipis = sorted({v["ine"] for v in info.values()})
    lot_de_municipi = {m: (i % 3) + 1 for i, m in enumerate(municipis)}
    execute_values(cur,
        "INSERT INTO ops.lot_cobertura (lot, codi_ine) VALUES %s",
        [(lot, m) for m, lot in lot_de_municipi.items()])

    execute_values(cur,
        "INSERT INTO ops.contractes (codi_expedient, lot, nif_empresa, objecte, "
        "data_inici, data_fi, import_adjudicat) VALUES %s",
        [(f"EXP-MANT-2023-{lot:02d}", lot, nif,
          f"Manteniment corrector de centres educatius. Lot {lot}",
          DATA_INICI, DATA_FI, float(rng.integers(1_200_000, 3_400_000)))
         for nif, _, lot in EMPRESES])
    empresa_de_lot = {lot: nif for nif, _, lot in EMPRESES}
    print(f"Empreses i contractes   : {len(EMPRESES)} lots")

    # ── Catalog de tipus ─────────────────────────────────────────────
    cur.execute("""SELECT codi_tipus, familia, prob_seguretat, prob_interrupcio,
                          pes_relatiu, cost_mitja FROM ops.tipus_incidencia
                   ORDER BY codi_tipus""")
    tipus = cur.fetchall()
    if not tipus:
        raise SystemExit("Cap tipus d'incidencia. Executa scripts/seed_tipus.py primer.")
    t_codis = [t[0] for t in tipus]
    t_pesos = np.array([float(t[4]) for t in tipus])
    t_pesos = t_pesos / t_pesos.sum()
    t_info = {t[0]: {"fam": t[1], "seg": float(t[2]),
                     "int": float(t[3]), "cost": float(t[5])} for t in tipus}

    # ── Repartiment d'incidencies per centre ─────────────────────────
    # Proporcional a la mida, pero amb exponent < 1 (els centres grans
    # no en generen proporcionalment tantes) i soroll lognormal que
    # reprodueix les excepcions: centres que en generen molt mes del
    # que els tocaria.
    codis = [c[0] for c in centres]
    pes = np.array([
        info[k]["alumnes"] ** 0.70
        * (1 + (2015 - info[k]["any"]) / 220)      # antiguitat: efecte feble
        * rng.lognormal(0, 0.38)                   # excepcions
        for k in codis
    ])
    pes = pes / pes.sum()
    per_centre = rng.multinomial(args.incidencies, pes)

    dies, w_clima, w_altres = construeix_calendari()
    dies_np = np.array(dies)

    # ── Genera les incidencies ───────────────────────────────────────
    ara = datetime(2026, 8, 23, tzinfo=timezone.utc)
    incidencies, actuacions_tmp = [], []

    for codi, n in zip(codis, per_centre):
        if n == 0:
            continue
        tips = rng.choice(t_codis, size=n, p=t_pesos)
        lot = lot_de_municipi[info[codi]["ine"]]

        for tp in tips:
            ti = t_info[tp]
            pesos_dia = w_clima if ti["fam"] == "CLIMATITZACIO" else w_altres
            dia = dies_np[rng.choice(len(dies_np), p=pesos_dia)]
            obertura = datetime(dia.year, dia.month, dia.day,
                                int(rng.integers(7, 19)), int(rng.integers(0, 60)),
                                tzinfo=timezone.utc)

            seguretat = bool(rng.random() < ti["seg"])
            interrupcio = bool(rng.random() < ti["int"])
            urgent = seguretat or interrupcio

            # SLA: 24h si urgent, 120h la resta. Compliment ~85%.
            limit = 24 if urgent else 120
            compleix = rng.random() < 0.85
            hores = (rng.uniform(1, limit) if compleix
                     else limit * rng.uniform(1.05, 3.2))
            assignacio = obertura + timedelta(hours=float(rng.uniform(0.2, 6)))

            prov = definitiva = None
            if urgent:
                prov = obertura + timedelta(hours=float(hores))
                # El 50% de les urgents necessiten intervencio definitiva
                # posterior; la resta queden resoltes de cop.
                if rng.random() < 0.50:
                    definitiva = prov + timedelta(days=float(rng.uniform(3, 55)))
                else:
                    definitiva = prov
            else:
                definitiva = obertura + timedelta(hours=float(hores))

            # Les incidencies encara en curs son nomes les recents.
            if definitiva and definitiva > ara:
                definitiva = None
            if prov and prov > ara:
                prov = None

            if definitiva is not None:
                estat = "TANCADA" if rng.random() < 0.82 else "RESOLTA"
            elif prov is not None:
                estat = "RESOLTA_PROVISIONAL"
            else:
                estat = str(rng.choice(["OBERTA", "ASSIGNADA", "EN_CURS"],
                                       p=[0.35, 0.30, 0.35]))

            cost = round(max(15.0, ti["cost"] * float(rng.lognormal(0, 0.42))), 2)
            # UUID deterministic a partir del generador amb llavor fixa.
            # No fem servir uuid4() perque trencaria la reproducibilitat.
            uid = str(uuid.UUID(bytes=rng.bytes(16)))

            incidencies.append((
                uid, codi, tp, seguretat, interrupcio, estat, None,
                str(rng.choice(CANALS, p=CANALS_P)),
                obertura, assignacio, prov, definitiva, cost,
            ))
            actuacions_tmp.append((uid, empresa_de_lot[lot], prov, definitiva, cost))

    print(f"Incidencies generades   : {len(incidencies):,}")

    cur.execute("TRUNCATE ops.incidencies, ops.actuacions CASCADE")
    execute_values(cur,
        "INSERT INTO ops.incidencies (uuid_origen, codi_centre, codi_tipus, "
        "requereix_seguretat, interromp_activitat, estat, descripcio, canal_entrada, "
        "data_obertura, data_assignacio, data_resolucio_provisional, "
        "data_resolucio_definitiva, cost_estimat) VALUES %s",
        incidencies, page_size=1000)

    # ── Actuacions ───────────────────────────────────────────────────
    # Una per cada resolucio efectiva: si hi va haver provisional i
    # despres definitiva en dates diferents, son dues visites.
    # Normalitzem a text: segons la configuracio, psycopg2 pot retornar
    # els UUID com a objectes uuid.UUID o com a cadenes. Comparar tipus
    # diferents donaria un KeyError silencios a la meitat del proces.
    cur.execute("SELECT uuid_origen, id_incidencia FROM ops.incidencies")
    id_de_uuid = {str(k): v for k, v in cur.fetchall()}

    actuacions = []
    for uid, nif, prov, definitiva, cost in actuacions_tmp:
        idi = id_de_uuid[uid]
        moments = []
        if prov is not None:
            moments.append((prov, prov != definitiva))
        if definitiva is not None and definitiva != prov:
            moments.append((definitiva, False))
        elif definitiva is not None and prov is None:
            moments.append((definitiva, False))

        for moment, parcial in moments:
            c = cost * (0.35 if parcial else 1.0)
            hores = round(float(rng.uniform(0.5, 9)), 2)
            actuacions.append((
                idi, nif, moment, hores,
                round(c * 0.58, 2), round(c * 0.42, 2),
                not parcial, None,
            ))

    execute_values(cur,
        "INSERT INTO ops.actuacions (id_incidencia, nif_empresa, data_actuacio, "
        "hores_treball, cost_ma_obra, cost_materials, resolt, observacions) VALUES %s",
        actuacions, page_size=1000)
    print(f"Actuacions generades    : {len(actuacions):,}")

    # ── Projectes d'inversio i certificacions ────────────────────────
    cur.execute("TRUNCATE ops.projectes_inversio, ops.certificacions CASCADE")
    # Els projectes es concentren als centres en pitjor estat: es aixi
    # com es prioritza la inversio real.
    ordenats = sorted(codis, key=lambda k: info[k]["any"])
    n_proj = max(40, len(codis) // 4)
    tipol = [t[0] for t in TIPOLOGIES_INVERSIO]
    tipol_p = np.array([t[1] for t in TIPOLOGIES_INVERSIO])
    tipol_p = tipol_p / tipol_p.sum()

    projectes = []
    for i in range(n_proj):
        codi = ordenats[int(rng.integers(0, max(1, len(ordenats) // 2)))]
        j = int(rng.choice(len(tipol), p=tipol_p))
        _, _, mitja, desv = TIPOLOGIES_INVERSIO[j]
        import_previst = round(float(max(25_000, rng.normal(mitja, desv))), 2)
        any_ini = int(rng.integers(2023, 2027))
        durada = int(rng.integers(1, 4))
        estat = str(rng.choice(
            ["PLANIFICAT", "LICITACIO", "ADJUDICAT", "EN_EXECUCIO", "FINALITZAT", "ATURAT"],
            p=[0.14, 0.10, 0.12, 0.28, 0.31, 0.05]))
        projectes.append((f"PI-{any_ini}-{i:04d}", codi,
                          f"{tipol[j].replace('_', ' ').capitalize()} a centre {codi}",
                          tipol[j], any_ini, any_ini + durada, import_previst, estat))

    execute_values(cur,
        "INSERT INTO ops.projectes_inversio (codi_projecte, codi_centre, denominacio, "
        "tipologia, any_inici, any_previst_fi, import_previst, estat) VALUES %s",
        projectes)

    cur.execute("SELECT id_projecte, any_inici, import_previst, estat FROM ops.projectes_inversio")
    certificacions = []
    for idp, any_ini, previst, estat in cur.fetchall():
        # Grau d'execucio segons l'estat del projecte. Els finalitzats
        # arriben al 100% (o una mica per sota); els planificats, a zero.
        grau = {"PLANIFICAT": 0.0, "LICITACIO": 0.0, "ADJUDICAT": 0.06,
                "EN_EXECUCIO": float(rng.uniform(0.20, 0.78)),
                "FINALITZAT": float(rng.uniform(0.93, 1.02)),
                "ATURAT": float(rng.uniform(0.10, 0.45))}[estat]
        if grau <= 0:
            continue
        total = float(previst) * grau
        n_cert = int(rng.integers(1, 7))
        talls = np.sort(rng.uniform(0, 1, n_cert - 1)) if n_cert > 1 else np.array([])
        fraccions = np.diff(np.concatenate([[0], talls, [1]]))
        for k, frac in enumerate(fraccions, start=1):
            d = date(any_ini, 1, 1) + timedelta(days=int(rng.integers(30, 900)))
            certificacions.append((idp, k, d, d.year, round(total * float(frac), 2)))

    execute_values(cur,
        "INSERT INTO ops.certificacions (id_projecte, num_certificacio, "
        "data_certificacio, exercici, import_certificat) VALUES %s",
        certificacions, page_size=1000)
    print(f"Projectes d'inversio    : {len(projectes):,}")
    print(f"Certificacions          : {len(certificacions):,}")

    conn.commit()
    cur.close()
    conn.close()
    print("\nGeneracio completada.")


if __name__ == "__main__":
    main()
