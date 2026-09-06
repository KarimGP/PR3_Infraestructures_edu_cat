"""
GENERADOR DEL CATALEG DCAT-AP
==============================

Llegeix meta.datasets i meta.distribucions i genera:
  1. El cataleg en JSON-LD segons DCAT-AP (docs/catalog/catalog.jsonld)
  2. Les distribucions reals en CSV i Parquet (data/public/)

PER QUE JSON-LD
---------------
DCAT es un vocabulari RDF. JSON-LD es la serialitzacio que permet
expressar RDF amb sintaxi JSON, de manera que un consumidor pot
tractar-lo com un JSON normal i, alhora, una eina semantica pot
interpretar-ne les relacions. Es el format que recomana la Comissio
Europea per als portals de dades obertes.

El @context es la peça clau: mapeja les claus curtes (dct:title) als
seus URI complets, que es el que fa que el document sigui RDF valid i
no nomes un JSON amb noms bonics.

VOCABULARIS CONTROLATS
----------------------
Els temes i les frequencies no son text lliure: apunten als
vocabularis oficials de la UE. Un portal que indexi aquest cataleg
sabra que EDUC vol dir "Educacio, cultura i esport" sense haver
d'interpretar cap cadena.

Us:
    python scripts/generate_dcat.py
"""

from __future__ import annotations

import json
import os
from datetime import date
from pathlib import Path

import pandas as pd
import psycopg2

DIR_CATALEG = Path("docs/catalog")
DIR_DADES = Path("data/public")

# Vocabularis oficials de la UE
TEMA_URI = "http://publications.europa.eu/resource/authority/data-theme"
FREQ_URI = "http://publications.europa.eu/resource/authority/frequency"
MEDIA_TYPE = {
    "CSV": "https://www.iana.org/assignments/media-types/text/csv",
    "PARQUET": "https://www.iana.org/assignments/media-types/application/vnd.apache.parquet",
    "JSON": "https://www.iana.org/assignments/media-types/application/json",
}

CONTEXT = {
    "dcat": "http://www.w3.org/ns/dcat#",
    "dct": "http://purl.org/dc/terms/",
    "foaf": "http://xmlns.com/foaf/0.1/",
    "vcard": "http://www.w3.org/2006/vcard/ns#",
    "xsd": "http://www.w3.org/2001/XMLSchema#",
}


def connecta():
    return psycopg2.connect(
        host="localhost", port=5432,
        dbname=os.getenv("POSTGRES_DB", "infraedu"),
        user=os.getenv("POSTGRES_USER", "pr3"),
        password=os.getenv("POSTGRES_PASSWORD", "pr3_local_dev"),
    )


def exporta_distribucions(conn, datasets):
    """Genera els fitxers reals que el cataleg descriu.

    Un cataleg que apunta a fitxers inexistents es un cataleg
    incomplet: DCAT descriu recursos que han d'existir.
    """
    DIR_DADES.mkdir(parents=True, exist_ok=True)
    mides = {}

    for ds in datasets:
        id_ds, origen = ds["id_dataset"], ds["objecte_origen"]
        df = pd.read_sql(f"SELECT * FROM {origen}", conn)

        fitxer_csv = DIR_DADES / f"{id_ds}.csv"
        df.to_csv(fitxer_csv, index=False, encoding="utf-8")
        mides[(id_ds, "CSV")] = fitxer_csv.stat().st_size

        # Parquet nomes per als conjunts grans: en els petits no
        # compensa i afegeix una dependencia de lectura innecessaria.
        if len(df) > 5000:
            fitxer_pq = DIR_DADES / f"{id_ds}.parquet"
            df.to_parquet(fitxer_pq, index=False)
            mides[(id_ds, "PARQUET")] = fitxer_pq.stat().st_size

        print(f"  {id_ds}: {len(df):,} files")

    return mides


def construeix_cataleg(datasets, distribucions, mides):
    avui = date.today().isoformat()

    llista_datasets = []
    for ds in datasets:
        dists = [d for d in distribucions if d["id_dataset"] == ds["id_dataset"]]

        recursos = []
        for d in dists:
            mida = mides.get((ds["id_dataset"], d["format"]))
            recurs = {
                "@type": "dcat:Distribution",
                "dct:title": f"{ds['titol_ca']} ({d['format']})",
                "dcat:accessURL": {"@id": d["url_acces"]},
                "dcat:downloadURL": {"@id": d["url_acces"]},
                "dct:format": {"@id": MEDIA_TYPE.get(d["format"], "")},
                "dct:license": {"@id": ds["llicencia"]},
                "dct:modified": {"@value": avui, "@type": "xsd:date"},
            }
            if mida:
                recurs["dcat:byteSize"] = {
                    "@value": str(mida), "@type": "xsd:nonNegativeInteger"}
            recursos.append(recurs)

        llista_datasets.append({
            "@type": "dcat:Dataset",
            "@id": f"#{ds['id_dataset']}",
            "dct:identifier": ds["id_dataset"],
            "dct:title": [
                {"@value": ds["titol_ca"], "@language": "ca"},
                {"@value": ds["titol_es"], "@language": "es"},
            ],
            "dct:description": {"@value": ds["descripcio_ca"], "@language": "ca"},
            "dct:publisher": {"@type": "foaf:Agent", "foaf:name": ds["editor"]},
            "dcat:contactPoint": {
                "@type": "vcard:Organization",
                "vcard:hasURL": {"@id": ds["punt_contacte"]},
            },
            "dcat:theme": {"@id": f"{TEMA_URI}/{ds['tema_eu']}"},
            "dct:accrualPeriodicity": {"@id": f"{FREQ_URI}/{ds['frequencia']}"},
            "dcat:keyword": ds["paraules_clau"],
            "dct:temporal": {
                "@type": "dct:PeriodOfTime",
                "dcat:startDate": {
                    "@value": str(ds["cobertura_temporal_inici"]),
                    "@type": "xsd:date"},
                "dcat:endDate": {
                    "@value": str(ds["cobertura_temporal_fi"]),
                    "@type": "xsd:date"},
            },
            "dct:issued": {"@value": str(ds["data_creacio"]), "@type": "xsd:date"},
            "dct:modified": {"@value": avui, "@type": "xsd:date"},
            "dct:license": {"@id": ds["llicencia"]},
            "dcat:distribution": recursos,
        })

    return {
        "@context": CONTEXT,
        "@type": "dcat:Catalog",
        "dct:title": {
            "@value": "Cataleg d'infraestructures educatives de Catalunya",
            "@language": "ca"},
        "dct:description": {
            "@value": "Conjunts de dades sobre centres educatius, "
                      "manteniment i inversio en infraestructures "
                      "educatives. Els centres son dades reals derivades "
                      "del Registre de Centres Docents; la resta son "
                      "dades simulades amb finalitat demostrativa.",
            "@language": "ca"},
        "dct:publisher": {"@type": "foaf:Agent", "foaf:name": datasets[0]["editor"]},
        "dct:issued": {"@value": avui, "@type": "xsd:date"},
        "dct:language": [{"@id": "http://publications.europa.eu/resource/authority/language/CAT"}],
        "dcat:dataset": llista_datasets,
    }


def main():
    conn = connecta()

    datasets = pd.read_sql(
        "SELECT * FROM meta.datasets ORDER BY id_dataset", conn
    ).to_dict("records")
    distribucions = pd.read_sql(
        "SELECT * FROM meta.distribucions ORDER BY id_dataset, format", conn
    ).to_dict("records")

    print("Exportant distribucions...")
    mides = exporta_distribucions(conn, datasets)
    conn.close()

    print("\nGenerant cataleg DCAT-AP...")
    cataleg = construeix_cataleg(datasets, distribucions, mides)

    DIR_CATALEG.mkdir(parents=True, exist_ok=True)
    fitxer = DIR_CATALEG / "catalog.jsonld"
    fitxer.write_text(
        json.dumps(cataleg, indent=2, ensure_ascii=False), encoding="utf-8")

    print(f"  {fitxer} ({fitxer.stat().st_size:,} bytes)")
    print(f"  {len(datasets)} datasets, "
          f"{sum(len(d['dcat:distribution']) for d in cataleg['dcat:dataset'])} distribucions")


if __name__ == "__main__":
    main()