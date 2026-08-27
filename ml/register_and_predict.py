"""
REGISTRE DEL MODEL I GENERACIO DE PREDICCIONS
==============================================

Entrena el model guanyador (XGBoost) sobre TOTES les dades disponibles,
el registra al Model Registry de MLflow amb un alias, i genera
prediccions per als propers 30 dies cap a una taula de PostgreSQL que
Power BI podra consumir.

Per que reentrenar sobre tot?
La comparacio amb la baseline necessitava un conjunt de validacio
apartat. Un cop decidit quin model guanya, per predir el futur real
convé fer servir tota la informacio disponible: apartar els ultims 90
dies nomes tenia sentit per avaluar.

Us:
    python ml/register_and_predict.py
"""

from __future__ import annotations

import os

import mlflow
import numpy as np
import pandas as pd
import psycopg2
from psycopg2.extras import execute_values
from xgboost import XGBRegressor

MLFLOW_URI = "sqlite:///mlruns.db"
EXPERIMENT = "incidencies-manteniment"
NOM_REGISTRE = "prediccio-incidencies-manteniment"
DIES_PREDICCIO = 30

FEATURES = ["mes", "dia_setmana", "setmana_any", "trimestre",
            "es_cap_setmana", "es_periode_lectiu",
            "lag_1", "lag_7", "lag_14",
            "mitjana_mobil_7", "mitjana_mobil_30", "desviacio_mobil_30"]


def connecta():
    return psycopg2.connect(
        host="localhost", port=5432,
        dbname=os.getenv("POSTGRES_DB", "infraedu"),
        user=os.getenv("POSTGRES_USER", "pr3"),
        password=os.getenv("POSTGRES_PASSWORD", "pr3_local_dev"),
    )


def crea_taula(cur):
    """La taula de prediccions viu a un esquema propi: no es ni
    operacional (ops) ni transformacio (dbt), es sortida del model."""
    cur.execute("CREATE SCHEMA IF NOT EXISTS ml")
    cur.execute("""
        CREATE TABLE IF NOT EXISTS ml.prediccions_incidencies (
            data              DATE         NOT NULL,
            familia           TEXT         NOT NULL,
            prediccio         NUMERIC(8,2) NOT NULL,
            model_nom         TEXT         NOT NULL,
            model_versio      TEXT,
            generat_el        TIMESTAMPTZ  NOT NULL DEFAULT now(),
            PRIMARY KEY (data, familia)
        )
    """)
    cur.execute("GRANT SELECT ON ml.prediccions_incidencies TO powerbi_ro")
    cur.execute("GRANT USAGE ON SCHEMA ml TO powerbi_ro")


def main():
    mlflow.set_tracking_uri(MLFLOW_URI)
    mlflow.set_experiment(EXPERIMENT)

    conn = connecta()
    df = pd.read_sql(
        "SELECT * FROM dbt_marts.mart_ml_features ORDER BY familia, data", conn)
    df["data"] = pd.to_datetime(df["data"])

    totes_prediccions = []

    for familia in sorted(df["familia"].unique()):
        d = df[df["familia"] == familia].dropna(subset=FEATURES).copy()
        print(f"\n{familia}: entrenant sobre {len(d)} dies")

        with mlflow.start_run(run_name=f"{familia}-final"):
            model = XGBRegressor(n_estimators=400, max_depth=5,
                                 learning_rate=0.05, subsample=0.85,
                                 colsample_bytree=0.85,
                                 random_state=42, n_jobs=4)
            model.fit(d[FEATURES], d["num_incidencies"])

            mlflow.log_param("familia", familia)
            mlflow.log_param("model", "xgboost_final")
            mlflow.log_param("dies_entrenament", len(d))
            info = mlflow.xgboost.log_model(
                model, "model",
                registered_model_name=f"{NOM_REGISTRE}-{familia.lower()}")

        # ── Prediccio recursiva ──────────────────────────────────────
        # Per predir el dia N+1 cal el valor del dia N, que encara no
        # existeix. La solucio es alimentar el model amb les seves
        # propies prediccions. L'error s'acumula amb l'horitzo: es una
        # limitacio inherent, i per aixo nomes predim 30 dies.
        historic = d["num_incidencies"].tolist()
        ultima_data = d["data"].max()

        for i in range(1, DIES_PREDICCIO + 1):
            nova_data = ultima_data + pd.Timedelta(days=i)
            fila = {
                "mes": nova_data.month,
                "dia_setmana": nova_data.isoweekday(),
                "setmana_any": nova_data.isocalendar().week,
                "trimestre": (nova_data.month - 1) // 3 + 1,
                "es_cap_setmana": int(nova_data.isoweekday() >= 6),
                "es_periode_lectiu": int(nova_data.month not in (7, 8)),
                "lag_1": historic[-1],
                "lag_7": historic[-7],
                "lag_14": historic[-14],
                "mitjana_mobil_7": float(np.mean(historic[-7:])),
                "mitjana_mobil_30": float(np.mean(historic[-30:])),
                "desviacio_mobil_30": float(np.std(historic[-30:])),
            }
            pred = float(model.predict(pd.DataFrame([fila])[FEATURES])[0])
            pred = max(0.0, pred)
            historic.append(pred)
            totes_prediccions.append(
                (nova_data.date(), familia, round(pred, 2),
                 "xgboost", info.registered_model_version
                 if hasattr(info, "registered_model_version") else None))

        print(f"  {DIES_PREDICCIO} dies predits")

    with conn, conn.cursor() as cur:
        crea_taula(cur)
        cur.execute("TRUNCATE ml.prediccions_incidencies")
        execute_values(cur,
            "INSERT INTO ml.prediccions_incidencies "
            "(data, familia, prediccio, model_nom, model_versio) VALUES %s",
            totes_prediccions)
    conn.close()

    print(f"\n{len(totes_prediccions)} prediccions escrites a "
          f"ml.prediccions_incidencies")


if __name__ == "__main__":
    main()