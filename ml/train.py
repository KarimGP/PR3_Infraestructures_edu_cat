"""
ENTRENAMENT I COMPARACIO DE MODELS
===================================

Prediu el nombre d'incidencies diaries per familia d'averia.

METODOLOGIA
-----------
1. BASELINE PRIMER. Prediccio naif: el valor de fa 7 dies. Si els
   models no la baten, aixo es el resultat i s'ha de reportar. Un
   portfolio que amaga la baseline no es fiable.

2. SPLIT TEMPORAL, MAI ALEATORI. Entrenament fins a 30 dies abans del
   final; validacio, els ultims 30. Un split aleatori filtraria futur
   al passat i inflaria les metriques de manera fraudulenta.

3. TOT A MLFLOW: parametres, metriques, grafics i models.

Us:
    python ml/train.py
    python ml/train.py --familia CLIMATITZACIO
"""

from __future__ import annotations

import argparse
import os
import warnings

import matplotlib
matplotlib.use("Agg")          # sense finestra grafica
import matplotlib.pyplot as plt
import mlflow
import mlflow.sklearn
import mlflow.xgboost
import numpy as np
import pandas as pd
import psycopg2
from sklearn.metrics import mean_absolute_error, mean_squared_error
from xgboost import XGBRegressor

warnings.filterwarnings("ignore")

DIES_VALIDACIO = 90
MLFLOW_URI = "sqlite:///mlruns.db"
EXPERIMENT = "incidencies-manteniment"


def carrega_dades():
    conn = psycopg2.connect(
        host="localhost", port=5432,
        dbname=os.getenv("POSTGRES_DB", "infraedu"),
        user=os.getenv("POSTGRES_USER", "pr3"),
        password=os.getenv("POSTGRES_PASSWORD", "pr3_local_dev"),
    )
    df = pd.read_sql("""
        SELECT * FROM dbt_marts.mart_ml_features ORDER BY familia, data
    """, conn)
    conn.close()
    df["data"] = pd.to_datetime(df["data"])
    return df


def metriques(real, pred):
    """MAE, RMSE i MAPE. El MAPE s'ha de calcular nomes sobre valors
    diferents de zero: dividir per zero donaria infinit i els dies
    d'agost en tenen molts."""
    real = np.asarray(real, dtype=float)
    pred = np.asarray(pred, dtype=float)
    mask = real != 0
    mape = (np.mean(np.abs((real[mask] - pred[mask]) / real[mask])) * 100
            if mask.sum() else np.nan)
    return {
        "mae": float(mean_absolute_error(real, pred)),
        "rmse": float(np.sqrt(mean_squared_error(real, pred))),
        "mape": float(mape),
    }


def grafic(dates, real, prediccions, titol, fitxer):
    """Grafic de real contra prediccions. S'adjunta com a artefacte
    a MLflow: una metrica sense grafic amaga on falla el model."""
    plt.figure(figsize=(13, 5))
    plt.plot(dates, real, label="Real", color="black", linewidth=2)
    for nom, valors in prediccions.items():
        plt.plot(dates, valors, label=nom, linestyle="--", alpha=0.85)
    plt.title(titol)
    plt.xlabel("Data")
    plt.ylabel("Incidencies")
    plt.legend()
    plt.grid(alpha=0.3)
    plt.tight_layout()
    plt.savefig(fitxer, dpi=110)
    plt.close()


def entrena_familia(df_fam: pd.DataFrame, familia: str):
    print(f"\n{'=' * 60}\n{familia}\n{'=' * 60}")

    # Els primers 30 dies no tenen lags complets
    df_fam = df_fam.dropna(subset=["lag_1", "lag_7", "lag_14",
                                   "mitjana_mobil_30"]).reset_index(drop=True)

    tall = df_fam["data"].max() - pd.Timedelta(days=DIES_VALIDACIO)
    train = df_fam[df_fam["data"] <= tall]
    valid = df_fam[df_fam["data"] > tall]
    print(f"Entrenament: {len(train)} dies | Validacio: {len(valid)} dies")

    resultats = {}
    prediccions = {}

    # ── BASELINE ─────────────────────────────────────────────────────
    with mlflow.start_run(run_name=f"{familia}-baseline"):
        pred = valid["lag_7"].values
        m = metriques(valid["num_incidencies"], pred)
        mlflow.log_param("familia", familia)
        mlflow.log_param("model", "baseline_lag7")
        mlflow.log_param("descripcio", "Prediccio naif: valor de fa 7 dies")
        mlflow.log_metrics(m)
        resultats["baseline"] = m
        prediccions["Baseline (lag 7)"] = pred
        print(f"  Baseline  MAE={m['mae']:.2f}  RMSE={m['rmse']:.2f}")

    # ── XGBOOST ──────────────────────────────────────────────────────
    features = ["mes", "dia_setmana", "setmana_any", "trimestre",
                "es_cap_setmana", "es_periode_lectiu",
                "lag_1", "lag_7", "lag_14",
                "mitjana_mobil_7", "mitjana_mobil_30", "desviacio_mobil_30"]

    with mlflow.start_run(run_name=f"{familia}-xgboost"):
        params = dict(n_estimators=400, max_depth=5, learning_rate=0.05,
                      subsample=0.85, colsample_bytree=0.85,
                      random_state=42, n_jobs=4)
        model = XGBRegressor(**params)
        model.fit(train[features], train["num_incidencies"])
        pred = model.predict(valid[features])
        m = metriques(valid["num_incidencies"], pred)

        mlflow.log_param("familia", familia)
        mlflow.log_param("model", "xgboost")
        mlflow.log_params(params)
        mlflow.log_metrics(m)
        mlflow.xgboost.log_model(model, "model")

        # Importancia de variables: quina informacio fa servir el model
        imp = pd.Series(model.feature_importances_, index=features)
        imp = imp.sort_values(ascending=False)
        for nom, valor in imp.items():
            mlflow.log_metric(f"imp_{nom}", float(valor))

        resultats["xgboost"] = m
        prediccions["XGBoost"] = pred
        print(f"  XGBoost   MAE={m['mae']:.2f}  RMSE={m['rmse']:.2f}")
        print(f"  Top 3 variables: {', '.join(imp.head(3).index)}")

    # ── PROPHET ──────────────────────────────────────────────────────
    try:
        from prophet import Prophet

        with mlflow.start_run(run_name=f"{familia}-prophet"):
            dfp = train[["data", "num_incidencies"]].rename(
                columns={"data": "ds", "num_incidencies": "y"})
            p = Prophet(yearly_seasonality=True, weekly_seasonality=True,
                        daily_seasonality=False,
                        seasonality_mode="multiplicative",
                        changepoint_prior_scale=0.05)
            p.fit(dfp)
            futur = valid[["data"]].rename(columns={"data": "ds"})
            pred = p.predict(futur)["yhat"].clip(lower=0).values
            m = metriques(valid["num_incidencies"], pred)

            mlflow.log_param("familia", familia)
            mlflow.log_param("model", "prophet")
            mlflow.log_param("seasonality_mode", "multiplicative")
            mlflow.log_metrics(m)

            resultats["prophet"] = m
            prediccions["Prophet"] = pred
            print(f"  Prophet   MAE={m['mae']:.2f}  RMSE={m['rmse']:.2f}")
    except Exception as e:
        print(f"  Prophet   NO DISPONIBLE ({type(e).__name__})")
        mlflow.log_param("prophet_error", type(e).__name__)

    # ── Grafic comparatiu ────────────────────────────────────────────
    fitxer = f"ml/grafic_{familia.lower()}.png"
    grafic(valid["data"], valid["num_incidencies"].values,
           prediccions, f"Prediccions - {familia}", fitxer)

    # ── Veredicte ────────────────────────────────────────────────────
    millor = min(resultats.items(), key=lambda x: x[1]["mae"])
    base_mae = resultats["baseline"]["mae"]
    millora = 100 * (base_mae - millor[1]["mae"]) / base_mae
    print(f"  --> Millor: {millor[0]} (MAE {millor[1]['mae']:.2f}, "
          f"{millora:+.1f}% vs baseline)")

    return {"familia": familia, "millor_model": millor[0],
            "mae_millor": millor[1]["mae"], "mae_baseline": base_mae,
            "millora_pct": millora}


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--familia", default=None,
                    help="Entrena nomes aquesta familia")
    args = ap.parse_args()

    mlflow.set_tracking_uri(MLFLOW_URI)
    mlflow.set_experiment(EXPERIMENT)

    df = carrega_dades()
    families = [args.familia] if args.familia else sorted(df["familia"].unique())

    resum = [entrena_familia(df[df["familia"] == f].copy(), f)
             for f in families]

    print(f"\n{'=' * 72}\nRESUM\n{'=' * 72}")
    r = pd.DataFrame(resum)
    print(r.to_string(index=False))
    r.to_csv("ml/resultats.csv", index=False)

    guanyen = (r["millora_pct"] > 0).sum()
    print(f"\nModels que baten la baseline: {guanyen}/{len(r)}")
    if guanyen < len(r):
        print("Les families on la baseline guanya s'han de reportar "
              "igualment: es un resultat valid, no un fracas.")


if __name__ == "__main__":
    main()