"""
DADES MINIMES PER A LA CI
==========================

La CI no pot dependre de l'API de la Generalitat: seria lenta i
fallaria cada cop que el servei extern estigues caigut. Un pipeline
de CI ha de ser rapid i determinista.

Aquest script carrega el minim necessari perque els 19 models de dbt
compilin, s'executin i passin els 88 tests: unes poques files per
taula que satisfacin totes les constraints i relacions.

No valida la logica de negoci amb dades realistes (aixo ho fa el
generador complet en local); valida que el CODI dels models funciona.
"""

import os
import uuid
from datetime import datetime, timedelta, timezone

import psycopg2
from psycopg2.extras import execute_values

conn = psycopg2.connect(
    host="localhost", port=5432,
    dbname=os.environ["POSTGRES_DB"],
    user=os.environ["POSTGRES_USER"],
    password=os.environ["POSTGRES_PASSWORD"],
)
cur = conn.cursor()

# ── Geografia ────────────────────────────────────────────────────
cur.execute("INSERT INTO ops.comarques VALUES ('13','Barcelones','Barcelona')")
cur.execute("INSERT INTO ops.municipis (codi_ine, nom, codi_comarca) "
            "VALUES ('080193','Badalona','13')")

# ── Centres ──────────────────────────────────────────────────────
centres = [
    ("08000001", "Institut de prova", "080193", 41.4501, 2.2474,
     False, False, False, True, True, False, False, False, False,
     1985, 4200.0, 620, 65),
    ("08000002", "Escola de prova", "080193", 41.4489, 2.2412,
     False, True, True, False, False, False, False, False, False,
     2005, 2100.0, 310, 82),
    ("08000003", "Llar d'infants de prova", "080193", 41.4530, 2.2501,
     True, False, False, False, False, False, False, False, False,
     2015, 800.0, 75, 91),
]
execute_values(cur, """
    INSERT INTO ops.centres (codi_centre, denominacio, codi_ine,
        latitud, longitud,
        te_infantil_1c, te_infantil_2c, te_primaria, te_eso,
        te_batxillerat, te_fp_mitja, te_fp_superior, te_adults,
        te_especial, any_construccio, superficie_m2, num_alumnes,
        estat_conservacio)
    VALUES %s""", centres)

# ── Empreses i contractes ────────────────────────────────────────
cur.execute("INSERT INTO ops.empreses VALUES ('B00000001','Empresa CI','MANTENIMENT')")
cur.execute("""INSERT INTO ops.contractes (codi_expedient, lot, nif_empresa,
    objecte, data_inici, data_fi, import_adjudicat)
    VALUES ('EXP-CI-01', 1, 'B00000001', 'Manteniment CI',
            '2024-01-01', '2026-12-31', 100000)""")
cur.execute("INSERT INTO ops.lot_cobertura VALUES (1, '080193')")

# ── Tipus d'incidencia: un per familia ───────────────────────────
tipus = [
    ("FON-001", "FONTANERIA",    "Fuita",      0.05, 0.35, 0.2700,  320),
    ("CLI-001", "CLIMATITZACIO", "Caldera",    0.05, 0.75, 0.2700,  850),
    ("PAL-001", "PALETERIA",     "Humitats",   0.05, 0.15, 0.1600,  520),
    ("ELE-001", "ELECTRICITAT",  "Tall",       0.35, 0.95, 0.1500,  540),
    ("FUS-001", "FUSTERIA",      "Porta",      0.15, 0.20, 0.1100,  165),
    ("EST-001", "ESTRUCTURA",    "Esquerda",   0.75, 0.35, 0.0400, 1800),
]
execute_values(cur, """
    INSERT INTO ops.tipus_incidencia (codi_tipus, familia, descripcio,
        prob_seguretat, prob_interrupcio, pes_relatiu, cost_mitja)
    VALUES %s""", tipus)

# ── Incidencies: una per tipus, alternant urgent i normal ────────
base = datetime(2025, 3, 10, 9, 0, tzinfo=timezone.utc)
incidencies, actuacions = [], []

for i, (codi_tipus, _, _, _, _, _, _) in enumerate(tipus):
    urgent = i % 2 == 0
    obertura = base + timedelta(days=i)
    # Respectem els limits d'SLA perque el test de coherencia passi
    restabliment = obertura + timedelta(hours=8 if urgent else 60)
    definitiva = restabliment + (timedelta(days=5) if urgent else timedelta(0))

    incidencies.append((
        str(uuid.UUID(int=i + 1)),
        centres[i % 3][0],
        codi_tipus,
        urgent, False,
        "TANCADA",
        "WEB",
        obertura,
        obertura + timedelta(hours=1),
        restabliment,
        definitiva,
        500.0,
    ))

execute_values(cur, """
    INSERT INTO ops.incidencies (uuid_origen, codi_centre, codi_tipus,
        requereix_seguretat, interromp_activitat, estat, canal_entrada,
        data_obertura, data_assignacio, data_resolucio_provisional,
        data_resolucio_definitiva, cost_estimat)
    VALUES %s""", incidencies)

cur.execute("SELECT id_incidencia, data_resolucio_definitiva FROM ops.incidencies")
for idi, data_def in cur.fetchall():
    actuacions.append((idi, "B00000001", data_def, 3.5, 200.0, 150.0, True))

execute_values(cur, """
    INSERT INTO ops.actuacions (id_incidencia, nif_empresa, data_actuacio,
        hores_treball, cost_ma_obra, cost_materials, resolt)
    VALUES %s""", actuacions)

# ── Inversio ─────────────────────────────────────────────────────
cur.execute("""INSERT INTO ops.projectes_inversio (codi_projecte, codi_centre,
    denominacio, tipologia, any_inici, any_previst_fi, import_previst, estat)
    VALUES ('PI-CI-0001', '08000001', 'Reforma CI', 'REFORMA',
            2024, 2026, 200000, 'EN_EXECUCIO')""")
cur.execute("""INSERT INTO ops.certificacions (id_projecte, num_certificacio,
    data_certificacio, exercici, import_certificat)
    SELECT id_projecte, 1, '2025-06-15', 2025, 80000
    FROM ops.projectes_inversio WHERE codi_projecte = 'PI-CI-0001'""")

# ── Stream ───────────────────────────────────────────────────────
cur.execute("""INSERT INTO bronze.raw_events (event_id, event_type, event_ts,
    codi_centre, payload, kafka_topic, kafka_partition, kafka_offset)
    VALUES (gen_random_uuid(), 'LECTURA_SENSOR', now(), '08000001',
            '{"magnitud":"TEMPERATURA","valor":21.5,"unitat":"C"}',
            'infraedu.events', 0, 1)""")
cur.execute("""INSERT INTO bronze.agg_events_5min (finestra_inici, finestra_fi,
    codi_centre, event_type, num_events, valor_mitja, valor_max)
    VALUES (date_trunc('hour', now()), date_trunc('hour', now()) + interval '5 min',
            '08000001', 'LECTURA_SENSOR', 1, 21.5, 21.5)""")

conn.commit()

cur.execute("SELECT count(*) FROM ops.incidencies")
print(f"Incidencies: {cur.fetchone()[0]}")
cur.execute("SELECT count(*) FROM ops.centres")
print(f"Centres    : {cur.fetchone()[0]}")

conn.close()
print("Dades de CI carregades.")