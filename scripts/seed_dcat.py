"""
CATALEG DCAT-AP: metadades dels datasets publicables
=====================================================

Pobla meta.datasets i meta.distribucions amb la descripcio dels
conjunts de dades que el projecte publica.

DCAT-AP es el perfil europeu d'aplicacio de DCAT (Data Catalog
Vocabulary), l'estandard del W3C per descriure catalegs de dades
obertes. Es el que fa servir el portal de la Generalitat d'on venen
els centres docents, i el que permet que data.europa.eu indexi
automaticament els datasets de qualsevol administracio.

Els vocabularis son controlats: els temes surten del data-theme de la
UE i les frequencies de l'ADMS. No son etiquetes lliures.

Us:
    python scripts/seed_dcat.py
"""

import os

import psycopg2
from psycopg2.extras import execute_values

EDITOR = "Projecte PR3 - Portfolio de data engineering"
CONTACTE = "https://github.com/KarimGP/PR3_Infraestructures_edu_cat"
LLICENCIA = "https://creativecommons.org/licenses/by/4.0/"
BASE_URL = ("https://raw.githubusercontent.com/KarimGP/"
            "PR3_Infraestructures_edu_cat/main/data/public")

# (id, titol_ca, titol_es, descripcio, frequencia, tema, paraules,
#  cobertura_inici, cobertura_fi, objecte_origen)
DATASETS = [
    (
        "centres-educatius-catalunya",
        "Centres educatius de Catalunya",
        "Centros educativos de Cataluña",
        "Directori de centres docents de Catalunya amb ubicacio, "
        "titularitat i ensenyaments autoritzats. Dades derivades del "
        "Registre de Centres Docents de la Generalitat de Catalunya. "
        "Inclou classificacio per tipus de centre i atributs "
        "d'infraestructura de caracter simulat.",
        "ANNUAL", "EDUC",
        ["educacio", "centres docents", "infraestructures", "catalunya"],
        "2025-09-01", "2026-08-31",
        "dbt_marts.dim_centre",
    ),
    (
        "incidencies-manteniment-educatiu",
        "Incidencies de manteniment en centres educatius",
        "Incidencias de mantenimiento en centros educativos",
        "Registre d'incidencies de manteniment correctiu en centres "
        "educatius del Barcelones, amb tipologia d'averia, temps de "
        "resolucio i compliment d'acords de nivell de servei. "
        "DADES SIMULADES amb patrons operatius reals del sector.",
        "DAILY", "EDUC",
        ["manteniment", "incidencies", "sla", "infraestructures"],
        "2023-09-01", "2026-08-31",
        "dbt_marts.fct_incidencies",
    ),
    (
        "inversio-infraestructures-educatives",
        "Inversio en infraestructures educatives",
        "Inversion en infraestructuras educativas",
        "Projectes d'inversio plurianual en centres educatius amb "
        "import previst, certificacions d'obra i grau d'execucio "
        "pressupostaria per tipologia d'actuacio. DADES SIMULADES.",
        "MONTHLY", "GOVE",
        ["inversio publica", "pressupost", "obra publica", "educacio"],
        "2023-01-01", "2028-12-31",
        "dbt_marts.fct_certificacions",
    ),
    (
        "prediccions-incidencies-manteniment",
        "Prediccions d'incidencies de manteniment",
        "Predicciones de incidencias de mantenimiento",
        "Prediccio a 30 dies del volum d'incidencies de manteniment "
        "per familia d'averia, generada amb un model XGBoost validat "
        "contra una baseline naif. Sortida d'un model predictiu, no "
        "dades observades.",
        "MONTHLY", "EDUC",
        ["prediccio", "machine learning", "manteniment"],
        "2026-09-01", "2026-09-30",
        "ml.prediccions_incidencies",
    ),
]

# (id_dataset, format, mida_aproximada)
DISTRIBUCIONS = [
    ("centres-educatius-catalunya", "CSV"),
    ("centres-educatius-catalunya", "PARQUET"),
    ("incidencies-manteniment-educatiu", "CSV"),
    ("incidencies-manteniment-educatiu", "PARQUET"),
    ("inversio-infraestructures-educatives", "CSV"),
    ("prediccions-incidencies-manteniment", "CSV"),
]

EXTENSIONS = {"CSV": "csv", "PARQUET": "parquet", "JSON": "json"}


def main():
    conn = psycopg2.connect(
        host="localhost", port=5432,
        dbname=os.getenv("POSTGRES_DB", "infraedu"),
        user=os.getenv("POSTGRES_USER", "pr3"),
        password=os.getenv("POSTGRES_PASSWORD", "pr3_local_dev"),
    )

    files_datasets = [
        (d[0], d[1], d[2], d[3], EDITOR, CONTACTE, LLICENCIA,
         d[4], d[5], d[6], d[7], d[8], d[9])
        for d in DATASETS
    ]

    files_distribucions = [
        (id_ds, fmt, f"{BASE_URL}/{id_ds}.{EXTENSIONS[fmt]}")
        for id_ds, fmt in DISTRIBUCIONS
    ]

    with conn, conn.cursor() as cur:
        cur.execute("TRUNCATE meta.datasets CASCADE")
        execute_values(cur,
            "INSERT INTO meta.datasets (id_dataset, titol_ca, titol_es, "
            "descripcio_ca, editor, punt_contacte, llicencia, frequencia, "
            "tema_eu, paraules_clau, cobertura_temporal_inici, "
            "cobertura_temporal_fi, objecte_origen) VALUES %s",
            files_datasets)

        execute_values(cur,
            "INSERT INTO meta.distribucions (id_dataset, format, url_acces) "
            "VALUES %s",
            files_distribucions)

    conn.close()
    print(f"Datasets      : {len(DATASETS)}")
    print(f"Distribucions : {len(DISTRIBUCIONS)}")


if __name__ == "__main__":
    main()