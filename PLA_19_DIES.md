# PR3 — Pla d'execució: 17 d'agost → 4 de setembre

**Regla que et salvarà el projecte:** cada dia acaba amb un commit que
deixi el repositori en un estat que *arrenca*. Si el dia 12 tens un
problema greu, has de poder ensenyar el dia 11. Un portfolio amb 3
projectes acabats val més que un amb 2 acabats i un a mitges.

**Segona regla:** els dies marcats amb 🛟 són marge deliberat. No els
omplis de feina nova. Si vas bé, els fas servir per polir; si vas
malament, per recuperar. Amb Flink pel mig, els necessitaràs.

---

## SETMANA 1 · Infraestructura (17–23 agost)

### Dia 1 — Dilluns 17 · Fonaments i base de dades

- [ ] Crear repo `PR3_Infraestructures_educatives` a GitHub, clonar-lo.
- [ ] Estructura de carpetes:
  ```
  ├── docker-compose.yml
  ├── .env.example          ← al repo
  ├── .env                  ← al .gitignore
  ├── docker/flink/Dockerfile
  ├── sql/01_schema.sql
  ├── flink_jobs/
  ├── producer/
  ├── dbt_infraedu/
  ├── ml/
  ├── docs/
  └── powerbi/
  ```
- [ ] **`.gitattributes` amb `* text=auto eol=lf`.** Git Bash a Windows
      converteix finals de línia a CRLF, i qualsevol `.sh` muntat dins
      d'un contenidor Linux fallarà amb `bad interpreter: No such file or
      directory`. És un error que et pot costar dues hores i sembla
      qualsevol cosa menys el que és.
- [ ] Comprovar que Docker Desktop fa servir el backend WSL2 i té com a
      mínim 8 GB de RAM assignats (Settings → Resources). Flink +
      Kafka + Postgres alhora amb 4 GB va molt just.
- [ ] `docker compose up -d postgres` i verificar amb DBeaver o
      `docker exec -it pr3-postgres psql -U pr3 -d infraedu -c "\dt ops.*"`.

> ⚠️ El directori `/docker-entrypoint-initdb.d` **només s'executa quan el
> volum es crea de zero**. Si modifiques `01_schema.sql` després,
> cal `docker compose down -v` per veure els canvis. Ho oblidaràs
> almenys un cop.

#### 🔴 PROVA DE FOC — la tasca més important dels 19 dies

No acabis el dia 1 sense això. El risc real del projecte no és dbt ni
l'ML: és que Flink no aconsegueixi escriure a Postgres. Val més
descobrir-ho avui, amb 18 dies per davant, que el dia 5.

- [ ] `docker compose build flink-jobmanager` (baixa els JARs, triga uns
      minuts la primera vegada).
- [ ] `docker compose up -d flink-jobmanager flink-taskmanager`.
- [ ] UI a http://localhost:8082 → **ha de mostrar 1 TaskManager i 4 slots**.
      Si en mostra 0, atura't aquí: res del que facis després funcionarà,
      i els missatges d'error no t'ho diran.
- [ ] Executar `flink_jobs/00_smoke_jdbc.py`:
      ```bash
      docker exec -it pr3-flink-jm ./bin/flink run -py /opt/flink_jobs/00_smoke_jdbc.py
      ```
- [ ] Verificar:
      ```bash
      docker exec -it pr3-postgres psql -U pr3 -d infraedu \
          -c "SELECT count(*), max(id) FROM bronze.smoke_test;"
      ```
      Esperat: **20 files, id màxim 20**.

Aquest job fa servir el connector `datagen` integrat, **no Kafka**. És
deliberat: aïlla la peça de risc. Si falla, només pot ser una de quatre
coses (JARs no carregats, TaskManager no registrat, xarxa Docker,
credencials), i les descartes en minuts. Amb Kafka pel mig tindries el
doble de superfície d'error.

**Si funciona:** la resta del streaming és construir a sobre de fonaments
que ja saps que aguanten. **Si no funciona:** tens tota la setmana per
replantejar, i el pla de retallada del final d'aquest document és la
teva xarxa de seguretat.

**Fita del dia:** 16 taules a Postgres **i una fila escrita per Flink**.

---

### Dia 2 — Dimarts 18 · Dades de referència i sintètiques

- [ ] Descarregar dades reals de centres educatius del portal de dades
      obertes de la Generalitat (analisi.transparenciacatalunya.cat) i
      del padró municipal per als municipis. Reutilitza el client d'API
      que ja vas fer al PR1.
- [ ] `scripts/seed_reference.py` → carrega comarques, municipis, centres.
- [ ] `scripts/generate_synthetic.py` amb Faker + numpy → incidències,
      actuacions, projectes, certificacions.

  **Important per al model d'ML del dia 13:** no generis dades
  aleatòries planes. Injecta-hi senyal real que el model pugui aprendre:
  estacionalitat (climatització dispara a juliol i gener), correlació
  entre `any_construccio` antic i més incidències estructurals,
  tendència creixent, i soroll. Si les dades són pur soroll, cap model
  batrà la mitjana i el projecte d'ML quedarà buit.

- [ ] Volum objectiu: ~600 centres, ~80.000 incidències repartides en
      3 anys, ~150.000 actuacions, ~400 projectes d'inversió.
- [ ] Validar amb consultes de coherència (cap SLA negatiu, cap
      certificació que superi el previst en més d'un 20%...).

**Fita:** OLTP poblat i creïble.

---

### Dia 3 — Dimecres 19 · Kafka i productor

- [ ] `docker compose up -d zookeeper kafka kafka-ui`.
- [ ] Verificar a http://localhost:8081 que el clúster es veu sa.
- [ ] Crear el topic:
      `docker exec pr3-kafka kafka-topics --create --topic infraedu.events --bootstrap-server localhost:29092 --partitions 3 --replication-factor 1`
- [ ] `producer/simulator.py` amb `confluent-kafka` o `kafka-python`.
      Emet esdeveniments JSON:
      - `INCIDENCIA_OBERTA` — nova avaria
      - `LECTURA_SENSOR` — temperatura/consum per centre
      - `ACTUACIO_REGISTRADA` — visita d'un operari
      - `CANVI_ESTAT` — transició d'estat d'una incidència

      Clau del missatge = `codi_centre` (garanteix ordre per centre dins
      d'una partició). Camps mínims: `event_id` (UUID), `event_type`,
      `event_ts` (ISO 8601 amb timezone), `codi_centre`, `payload`.

- [ ] Argument `--rate` per controlar esdeveniments/segon i `--seed`
      per reproduir execucions.
- [ ] Consumir des de consola per confirmar que hi arriben.

> ⚠️ Des de Windows el productor ha d'apuntar a `localhost:9092`, mai a
> `kafka:29092`. Aquesta distinció està explicada al `docker-compose.yml`.

**Fita:** esdeveniments visibles a Kafka UI.

---

### Dia 4 — Dijous 20 · Flink en marxa

El clúster ja el vas validar el dia 1 amb la prova de foc. Avui només
hi afegeixes Kafka com a font: el sink JDBC ja saps que funciona, així
que si alguna cosa peta, el problema és del connector de Kafka i prou.

- [ ] Confirmar que el clúster segueix sa (UI, 1 TaskManager, 4 slots).
- [ ] Primer job PyFlink amb Kafka: llegir el topic i fer `print`.
      Executar-lo **dins del contenidor**:
      ```bash
      docker exec -it pr3-flink-jm ./bin/flink run -py /opt/flink_jobs/01_kafka_to_print.py
      ```
- [ ] Llegir la sortida amb `docker compose logs -f flink-taskmanager`
      (el `print` surt al TaskManager, no al JobManager: és la primera
      confusió clàssica de PyFlink).

**Fita:** veure esdeveniments passant per Flink.

---

### Dia 5 — Divendres 21 · Flink → Bronze

- [ ] `flink_jobs/02_kafka_to_postgres.py`: Table API amb connector
      `kafka` com a font i `jdbc` com a sink cap a `bronze.raw_events`.
- [ ] Definir `event_ts` com a columna de temps d'esdeveniment amb
      `WATERMARK FOR event_ts AS event_ts - INTERVAL '10' SECOND`.
- [ ] Activar checkpointing (ja ve configurat a 30s al compose) i
      comprovar que apareixen checkpoints correctes a la UI.

> ⚠️ El sink JDBC fa micro-batch. Per defecte no veuràs res a Postgres
> fins que s'acumulin prou files o passi l'interval. Baixa
> `sink.buffer-flush.interval` a `2s` mentre desenvolupes o pensaràs
> que el job no funciona.

**Fita:** files a `bronze.raw_events` amb offset i partició de Kafka.

---

### Dia 6 — Dissabte 22 · Agregació amb finestres

Aquest és el dia que justifica tenir Flink al projecte. Un sink directe
el faria qualsevol consumidor Python; una finestra amb estat, no.

- [ ] `flink_jobs/03_windowed_agg.py`: `TUMBLE` de 5 minuts agrupant
      per `codi_centre` i `event_type` → `bronze.agg_events_5min`.
- [ ] Sink en mode upsert (definir PK a la DDL de la taula Flink) per
      evitar duplicats si el job es reinicia.
- [ ] Documentar a `docs/streaming.md`: per què event time i no
      processing time, què fa el watermark, què passa amb els
      esdeveniments que arriben tard.

**Fita:** dues taules Bronze alimentades en continu.

---

### Dia 7 — Diumenge 23 · 🛟 Consolidació

- [ ] `Makefile` o `scripts/*.sh`: `make up`, `make seed`, `make stream`,
      `make down`.
- [ ] `docs/arquitectura.md` amb diagrama (Mermaid o draw.io).
- [ ] Prova de foc: `docker compose down -v` i aixecar-ho tot des de
      zero seguint només el README. Si falla algun pas, el README és
      incorrecte. **Aquesta prova és la diferència entre un repo que
      algú pot executar i un que no.**

---

## SETMANA 2 · Transformació i models (24–30 agost)

### Dia 8 — Dilluns 24 · dbt: arrencada i staging

- [ ] `pip install dbt-core dbt-postgres` al venv, `dbt init dbt_infraedu`.
- [ ] `profiles.yml` amb variables d'entorn (`{{ env_var('POSTGRES_PASSWORD') }}`),
      mai credencials en clar.
- [ ] `sources.yml` declarant `ops` i `bronze` amb `freshness` i
      `loaded_at_field`.
- [ ] Models `stg_*`: un per taula font. Només neteja i renaming, sense
      lògica de negoci. Materialització `view`.
- [ ] Convenció de noms documentada al README.

### Dia 9 — Dimarts 25 · dbt: intermediate i marts

- [ ] `int_incidencies_sla`: calcula temps de resolució i compliment de SLA.
- [ ] `int_projectes_execucio`: certificat vs previst per projecte.
- [ ] Marts en esquema en estrella:
      - `dim_centre`, `dim_temps`, `dim_empresa`, `dim_tipus_incidencia`
      - `fct_incidencies`, `fct_actuacions`, `fct_certificacions`
      - `fct_events_streaming` (des de Bronze)
- [ ] `dim_temps` generada amb `dbt_utils.date_spine`.
- [ ] Materialitzar les fact com a `incremental` amb `unique_key`.
      És l'oportunitat de demostrar que entens càrregues incrementals.

### Dia 10 — Dimecres 26 · dbt: tests

- [ ] Genèrics: `unique`, `not_null`, `relationships`, `accepted_values`
      a totes les dimensions i claus foranes.
- [ ] `dbt_utils` / `dbt_expectations` per rangs i cardinalitats.
- [ ] Tests singulars a `tests/`:
      - cap incidència resolta abans d'obrir-se
      - suma de certificacions ≤ import previst × 1.2
      - cap centre òrfe a les fact
      - continuïtat de la sèrie temporal (cap dia buit)
- [ ] Objectiu: **més de 25 tests**, tots verds. Al PR2 en tenies 7;
      aquest salt és un argument concret a l'entrevista.

### Dia 11 — Dijous 27 · dbt: documentació i llinatge

- [ ] Descripcions a tots els models i columnes crítiques.
- [ ] `dbt docs generate` i publicar el DAG a GitHub Pages.
      **Un gràfic de llinatge navegable és el que més impressiona d'un
      projecte dbt.** No t'ho saltis per falta de temps.
- [ ] `exposures.yml` declarant el dashboard de Power BI: tanca el
      llinatge d'extrem a extrem.

### Dia 12 — Divendres 28 · Preparació del dataset d'ML

- [ ] Model `mart_ml_features`: sèrie diària d'incidències per centre i
      família, amb lags (1, 7, 30), mitjanes mòbils, variables de
      calendari (dia de la setmana, mes, festius escolars) i atributs
      del centre (antiguitat, superfície, alumnes).
- [ ] Split temporal, mai aleatori: entrenament fins a 30 dies abans del
      final, validació els últims 30. Amb sèries temporals, un split
      aleatori filtra futur al passat i infla les mètriques.
- [ ] Anàlisi exploratòria a `notebooks/01_eda.ipynb`.

### Dia 13 — Dissabte 29 · Prophet vs XGBoost amb MLflow

- [ ] Baseline primer: predicció naïf (valor de fa 7 dies). **Si els
      models no la baten, això és el resultat i s'ha de dir.** Un
      portfolio que reporta un model dolent honestament és més fiable
      que un que amaga la baseline.
- [ ] Prophet amb estacionalitat setmanal i anual + festius.
- [ ] XGBoost sobre les features tabulars.
- [ ] Tot registrat a MLflow: paràmetres, MAE/RMSE/MAPE, gràfics,
      artefactes del model.

> ⚠️ Recorda l'URI de SQLite de MLflow 3.x: `sqlite:///mlruns.db`
> relatiu, o `sqlite:////ruta/absoluta` amb quatre barres.

### Dia 14 — Diumenge 30 · 🛟 Tancament d'ML

- [ ] Registrar el millor model al Model Registry amb alias.
- [ ] `ml/predict.py` → escriu prediccions a `marts.fct_prediccions`.
- [ ] `docs/ml.md`: metodologia, resultats vs baseline, limitacions.

---

## SETMANA 3 · Presentació (31 agost – 4 setembre)

### Dia 15 — Dilluns 31 · Power BI: model i pàgines 1–2

- [ ] Connector PostgreSQL amb l'usuari `powerbi_ro`, en **Import**
      (DirectQuery contra el teu portàtil no aporta res i complica).
- [ ] Model en estrella, relacions 1:N des de les dimensions, marcar
      `dim_temps` com a taula de dates.
- [ ] Taula de mesures dedicada, DAX documentat.
- [ ] Pàgina 1: visió executiva (KPIs, mapa de centres).
- [ ] Pàgina 2: incidències i compliment de SLA.

> ⚠️ El problema de decimals amb la configuració regional espanyola que
> vas tenir al PR2: fixa el tipus de dades a la consulta amb
> `Locale = "en-US"` per als numèrics que vinguin de text.

### Dia 16 — Dimarts 1 · Power BI: pàgines 3–5

- [ ] Pàgina 3: execució pressupostària (previst vs certificat).
- [ ] Pàgina 4: monitor de streaming (des de `bronze.agg_events_5min`).
- [ ] Pàgina 5: prediccions vs real amb interval de confiança.
- [ ] Publicar a Power BI Service i capturar imatges per al README.

### Dia 17 — Dimecres 2 · DCAT i dades obertes

- [ ] Poblar `meta.datasets` i `meta.distribucions` per als 4–5 marts
      publicables.
- [ ] `scripts/generate_dcat.py` → catàleg **DCAT-AP** en JSON-LD i
      Turtle a `docs/catalog/`.
- [ ] Exportar les distribucions reals (CSV i Parquet) a `data/public/`.
- [ ] Validar el JSON-LD amb l'SHACL validator del portal de dades
      europeu i incloure la captura. **Aquest detall et distingirà:
      molta gent diu "DCAT" i ningú valida.**
- [ ] `docs/dcat.md` explicant el mapatge camp a camp.

### Dia 18 — Dijous 3 · Documentació

- [ ] README principal: problema, arquitectura, stack, com executar-ho,
      resultats, decisions i limitacions.
- [ ] **Secció "Decisions d'arquitectura"** amb els perquès: Kafka amb
      Zookeeper i no KRaft, dbt sobre Postgres i no DuckDB, event time
      i no processing time. Explicar *per què* val més que la llista d'eines.
- [ ] Diagrama d'arquitectura definitiu.
- [ ] GIF curt del pipeline en marxa (productor → Kafka UI → Flink UI →
      files apareixent a Postgres). Val més que deu paràgrafs.

### Dia 19 — Divendres 4 · Publicació

- [ ] GitHub Actions: `dbt deps` + `dbt compile` a cada push (CI real,
      encara que sigui mínima).
- [ ] Repàs final: `.env` fora del repo, cap credencial al codi,
      llicència, `requirements.txt` amb versions fixades.
- [ ] Prova neta final des de zero en un directori nou.
- [ ] Integrar el projecte a karimgp.com.
- [ ] Publicar a LinkedIn.

---

## Si vas endarrerit: ordre de retallada

Retalla en aquest ordre, i **documenta la decisió al README** en
comptes d'amagar-la. Explicar per què has acotat un abast és
exactament el que fa un enginyer sènior.

1. Pàgines 4 i 5 de Power BI → deixa-ho en 3.
2. Turtle del DCAT → només JSON-LD.
3. Materialitzacions incrementals → `table` simple.
4. Prophet → només XGBoost contra la baseline.
5. Finestra de Flink → només el sink directe a Bronze.

**El que no s'ha de retallar mai:** que `docker compose up` funcioni des
de zero, que els tests de dbt passin, i que el README sigui honest.
