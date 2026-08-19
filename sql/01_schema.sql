-- ============================================================================
-- PR3 · Sistema de gestió d'infraestructures educatives
-- Esquema PostgreSQL: capa operacional (OLTP) + landing de streaming
--
-- Tres esquemes, tres responsabilitats:
--   ops     → base de dades transaccional. Normalitzada (3NF). Aquí és on
--             viurien les dades si això fos un sistema real de gestió.
--   bronze  → aterratge cru del stream de Kafka. Sense transformar, amb
--             metadades de procedència (offset, partició, hora d'ingesta).
--   meta    → catàleg DCAT-AP. Metadades dels datasets publicables.
--
-- dbt llegirà de ops + bronze i materialitzarà staging/marts als esquemes
-- dbt_staging i dbt_marts, que crearà ell mateix.
-- ============================================================================

CREATE SCHEMA IF NOT EXISTS ops;
CREATE SCHEMA IF NOT EXISTS bronze;
CREATE SCHEMA IF NOT EXISTS meta;

-- Utilitat: manteniment automàtic de updated_at.
-- En un OLTP real això és imprescindible per fer CDC incremental després.
CREATE OR REPLACE FUNCTION ops.set_updated_at()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = now();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;


-- ============================================================================
-- 1. GEOGRAFIA I ENTITATS DE REFERÈNCIA
-- ============================================================================

CREATE TABLE ops.comarques (
    codi_comarca    VARCHAR(2)      PRIMARY KEY,
    nom             TEXT         NOT NULL UNIQUE,
    provincia       TEXT         NOT NULL
                                 CHECK (provincia IN ('Barcelona','Girona','Lleida','Tarragona'))
);

CREATE TABLE ops.municipis (
    codi_ine        VARCHAR(6)      PRIMARY KEY,
    nom             TEXT         NOT NULL,
    codi_comarca    VARCHAR(2)      NOT NULL REFERENCES ops.comarques(codi_comarca),
    poblacio        INTEGER      CHECK (poblacio >= 0),
    -- Coordenades del centroide, per als mapes de Power BI
    latitud         NUMERIC(9,6),
    longitud        NUMERIC(9,6)
);

CREATE INDEX idx_municipis_comarca ON ops.municipis(codi_comarca);


-- ============================================================================
-- 2. CENTRES EDUCATIUS (el "actiu" que gestionem)
-- ============================================================================

CREATE TABLE ops.centres (
    codi_centre     VARCHAR(8)      PRIMARY KEY,   -- format oficial del Dept. d'Educació
    nom             TEXT         NOT NULL,
    tipus           TEXT         NOT NULL
                                 CHECK (tipus IN ('INS','CEIP','ESCOLA','EOI','CFA','ZER','SES')),
    titularitat     TEXT         NOT NULL DEFAULT 'PUBLIC'
                                 CHECK (titularitat IN ('PUBLIC','CONCERTAT')),
    codi_ine        VARCHAR(6)      NOT NULL REFERENCES ops.municipis(codi_ine),
    adreca          TEXT,
    any_construccio SMALLINT     CHECK (any_construccio BETWEEN 1850 AND 2030),
    superficie_m2   NUMERIC(10,2) CHECK (superficie_m2 > 0),
    num_alumnes     INTEGER      CHECK (num_alumnes >= 0),
    -- Índex sintètic 0-100 de l'estat de conservació. El farem servir
    -- com a variable predictora al model d'ML.
    estat_conservacio SMALLINT   CHECK (estat_conservacio BETWEEN 0 AND 100),
    actiu           BOOLEAN      NOT NULL DEFAULT TRUE,
    created_at      TIMESTAMPTZ  NOT NULL DEFAULT now(),
    updated_at      TIMESTAMPTZ  NOT NULL DEFAULT now()
);

CREATE INDEX idx_centres_municipi ON ops.centres(codi_ine);
CREATE INDEX idx_centres_tipus    ON ops.centres(tipus) WHERE actiu;

CREATE TRIGGER trg_centres_updated
    BEFORE UPDATE ON ops.centres
    FOR EACH ROW EXECUTE FUNCTION ops.set_updated_at();


-- ============================================================================
-- 3. EMPRESES I CONTRACTES (els lots de manteniment)
-- ============================================================================

CREATE TABLE ops.empreses (
    nif             VARCHAR(12)  PRIMARY KEY,
    nom             TEXT         NOT NULL,
    tipus           TEXT         NOT NULL DEFAULT 'MANTENIMENT'
                                 CHECK (tipus IN ('MANTENIMENT','OBRA','SUBMINISTRAMENT','SERVEIS'))
);

CREATE TABLE ops.contractes (
    id_contracte    SERIAL       PRIMARY KEY,
    codi_expedient  TEXT         NOT NULL UNIQUE,
    lot             SMALLINT     NOT NULL CHECK (lot BETWEEN 1 AND 3),
    nif_empresa     VARCHAR(12)  NOT NULL REFERENCES ops.empreses(nif),
    objecte         TEXT         NOT NULL,
    data_inici      DATE         NOT NULL,
    data_fi         DATE         NOT NULL,
    import_adjudicat NUMERIC(14,2) NOT NULL CHECK (import_adjudicat >= 0),
    created_at      TIMESTAMPTZ  NOT NULL DEFAULT now(),
    CONSTRAINT chk_contracte_dates CHECK (data_fi > data_inici)
);

-- Quin lot cobreix quina comarca. Un lot pot cobrir diverses comarques.
CREATE TABLE ops.lot_cobertura (
    lot             SMALLINT     NOT NULL CHECK (lot BETWEEN 1 AND 3),
    codi_comarca    VARCHAR(2)      NOT NULL REFERENCES ops.comarques(codi_comarca),
    PRIMARY KEY (lot, codi_comarca)
);


-- ============================================================================
-- 4. INCIDÈNCIES I ACTUACIONS (el cor transaccional)
-- ============================================================================

CREATE TABLE ops.tipus_incidencia (
    codi_tipus      VARCHAR(10)  PRIMARY KEY,
    familia         TEXT         NOT NULL
                                 CHECK (familia IN ('CLIMATITZACIO','ELECTRICITAT','FONTANERIA',
                                                    'ESTRUCTURA','FUSTERIA','SEGURETAT','ALTRES')),
    descripcio      TEXT         NOT NULL,
    prioritat_defecte TEXT       NOT NULL
                                 CHECK (prioritat_defecte IN ('BAIXA','MITJANA','ALTA','CRITICA')),
    -- Acord de nivell de servei: hores màximes per resoldre.
    -- Ens permet calcular compliment de SLA als marts de dbt.
    sla_hores       INTEGER      NOT NULL CHECK (sla_hores > 0)
);

CREATE TABLE ops.incidencies (
    id_incidencia   BIGSERIAL    PRIMARY KEY,
    -- UUID que arriba del stream de Kafka; permet lligar bronze ↔ ops
    uuid_origen     UUID         UNIQUE,
    codi_centre     VARCHAR(8)      NOT NULL REFERENCES ops.centres(codi_centre),
    codi_tipus      VARCHAR(10)  NOT NULL REFERENCES ops.tipus_incidencia(codi_tipus),
    prioritat       TEXT         NOT NULL
                                 CHECK (prioritat IN ('BAIXA','MITJANA','ALTA','CRITICA')),
    estat           TEXT         NOT NULL DEFAULT 'OBERTA'
                                 CHECK (estat IN ('OBERTA','ASSIGNADA','EN_CURS','RESOLTA','TANCADA','ANULADA')),
    descripcio      TEXT,
    canal_entrada   TEXT         CHECK (canal_entrada IN ('TELEFON','WEB','SENSOR','INSPECCIO')),
    data_obertura   TIMESTAMPTZ  NOT NULL,
    data_assignacio TIMESTAMPTZ,
    data_resolucio  TIMESTAMPTZ,
    cost_estimat    NUMERIC(12,2) CHECK (cost_estimat >= 0),
    created_at      TIMESTAMPTZ  NOT NULL DEFAULT now(),
    updated_at      TIMESTAMPTZ  NOT NULL DEFAULT now(),
    -- Coherència temporal. Sense això, les dades sintètiques generaran
    -- SLA negatius i et passaràs una tarda depurant dbt en comptes del generador.
    CONSTRAINT chk_incidencia_temps
        CHECK (data_resolucio IS NULL OR data_resolucio >= data_obertura),
    CONSTRAINT chk_incidencia_resolta
        CHECK (estat NOT IN ('RESOLTA','TANCADA') OR data_resolucio IS NOT NULL)
);

CREATE INDEX idx_inc_centre     ON ops.incidencies(codi_centre);
CREATE INDEX idx_inc_obertura   ON ops.incidencies(data_obertura);
CREATE INDEX idx_inc_estat      ON ops.incidencies(estat) WHERE estat NOT IN ('TANCADA','ANULADA');
CREATE INDEX idx_inc_tipus      ON ops.incidencies(codi_tipus);

CREATE TRIGGER trg_inc_updated
    BEFORE UPDATE ON ops.incidencies
    FOR EACH ROW EXECUTE FUNCTION ops.set_updated_at();


CREATE TABLE ops.actuacions (
    id_actuacio     BIGSERIAL    PRIMARY KEY,
    id_incidencia   BIGINT       NOT NULL REFERENCES ops.incidencies(id_incidencia) ON DELETE CASCADE,
    nif_empresa     VARCHAR(12)  NOT NULL REFERENCES ops.empreses(nif),
    data_actuacio   TIMESTAMPTZ  NOT NULL,
    hores_treball   NUMERIC(6,2) NOT NULL CHECK (hores_treball > 0),
    cost_ma_obra    NUMERIC(12,2) NOT NULL DEFAULT 0 CHECK (cost_ma_obra >= 0),
    cost_materials  NUMERIC(12,2) NOT NULL DEFAULT 0 CHECK (cost_materials >= 0),
    resolt          BOOLEAN      NOT NULL DEFAULT FALSE,
    observacions    TEXT,
    created_at      TIMESTAMPTZ  NOT NULL DEFAULT now()
);

CREATE INDEX idx_act_incidencia ON ops.actuacions(id_incidencia);
CREATE INDEX idx_act_data       ON ops.actuacions(data_actuacio);


-- ============================================================================
-- 5. INVERSIÓ PLURIANUAL (marc pressupostari)
-- ============================================================================

CREATE TABLE ops.projectes_inversio (
    id_projecte     SERIAL       PRIMARY KEY,
    codi_projecte   TEXT         NOT NULL UNIQUE,
    codi_centre     VARCHAR(8)      NOT NULL REFERENCES ops.centres(codi_centre),
    denominacio     TEXT         NOT NULL,
    tipologia       TEXT         NOT NULL
                                 CHECK (tipologia IN ('AMPLIACIO','REFORMA','NOVA_CONSTRUCCIO',
                                                      'EFICIENCIA_ENERGETICA','ACCESSIBILITAT','RETIRADA_AMIANT')),
    any_inici       SMALLINT     NOT NULL,
    any_previst_fi  SMALLINT     NOT NULL,
    import_previst  NUMERIC(14,2) NOT NULL CHECK (import_previst > 0),
    estat           TEXT         NOT NULL DEFAULT 'PLANIFICAT'
                                 CHECK (estat IN ('PLANIFICAT','LICITACIO','ADJUDICAT','EN_EXECUCIO','FINALITZAT','ATURAT')),
    created_at      TIMESTAMPTZ  NOT NULL DEFAULT now(),
    updated_at      TIMESTAMPTZ  NOT NULL DEFAULT now(),
    CONSTRAINT chk_projecte_anys CHECK (any_previst_fi >= any_inici)
);

CREATE TRIGGER trg_proj_updated
    BEFORE UPDATE ON ops.projectes_inversio
    FOR EACH ROW EXECUTE FUNCTION ops.set_updated_at();

-- Certificacions: execució real contra el previst. La diferència entre
-- import_previst i la suma de certificacions és la mètrica estrella
-- de qualsevol quadre de comandament d'inversió pública.
CREATE TABLE ops.certificacions (
    id_certificacio BIGSERIAL    PRIMARY KEY,
    id_projecte     INTEGER      NOT NULL REFERENCES ops.projectes_inversio(id_projecte) ON DELETE CASCADE,
    num_certificacio SMALLINT    NOT NULL CHECK (num_certificacio > 0),
    data_certificacio DATE       NOT NULL,
    exercici        SMALLINT     NOT NULL,
    import_certificat NUMERIC(14,2) NOT NULL CHECK (import_certificat >= 0),
    created_at      TIMESTAMPTZ  NOT NULL DEFAULT now(),
    UNIQUE (id_projecte, num_certificacio)
);

CREATE INDEX idx_cert_projecte ON ops.certificacions(id_projecte);
CREATE INDEX idx_cert_exercici ON ops.certificacions(exercici);


-- ============================================================================
-- 6. BRONZE — aterratge del stream (Flink hi escriu, ningú més)
-- ============================================================================

-- Esdeveniments crus, tal com surten de Kafka. El payload es guarda
-- sencer en JSONB: si demà el productor afegeix un camp nou, no has de
-- tocar l'esquema ni perds informació. Això és el principi de la capa
-- Bronze i val la pena que ho expliquis així al README.
CREATE TABLE bronze.raw_events (
    event_id        UUID         PRIMARY KEY,
    event_type      TEXT         NOT NULL,
    event_ts        TIMESTAMPTZ  NOT NULL,
    codi_centre     VARCHAR(8),
    payload         JSONB        NOT NULL,
    -- Metadades de procedència: imprescindibles per depurar i per
    -- demostrar que entens el llinatge de dades.
    kafka_topic     TEXT,
    kafka_partition INTEGER,
    kafka_offset    BIGINT,
    ingested_at     TIMESTAMPTZ  NOT NULL DEFAULT now()
);

CREATE INDEX idx_raw_events_ts    ON bronze.raw_events(event_ts);
CREATE INDEX idx_raw_events_type  ON bronze.raw_events(event_type);
CREATE INDEX idx_raw_events_gin   ON bronze.raw_events USING GIN (payload);

-- Sortida de la finestra d'agregació de Flink (tumbling window de 5 min).
-- Aquesta taula és la que justifica tenir Flink: no és un simple
-- pass-through, hi ha computació d'estat sobre el stream.
CREATE TABLE bronze.agg_events_5min (
    finestra_inici  TIMESTAMPTZ  NOT NULL,
    finestra_fi     TIMESTAMPTZ  NOT NULL,
    codi_centre     VARCHAR(8)      NOT NULL,
    event_type      TEXT         NOT NULL,
    num_events      INTEGER      NOT NULL,
    valor_mitja     NUMERIC(12,4),
    valor_max       NUMERIC(12,4),
    processed_at    TIMESTAMPTZ  NOT NULL DEFAULT now(),
    PRIMARY KEY (finestra_inici, codi_centre, event_type)
);

CREATE INDEX idx_agg_finestra ON bronze.agg_events_5min(finestra_inici);


-- ============================================================================
-- 7. META — catàleg DCAT-AP
-- ============================================================================

CREATE TABLE meta.datasets (
    id_dataset      TEXT         PRIMARY KEY,   -- identificador persistent
    titol_ca        TEXT         NOT NULL,
    titol_es        TEXT,
    descripcio_ca   TEXT         NOT NULL,
    editor          TEXT         NOT NULL,
    punt_contacte   TEXT,
    llicencia       TEXT         NOT NULL DEFAULT 'https://creativecommons.org/licenses/by/4.0/',
    frequencia      TEXT,                       -- vocabulari EU: DAILY, MONTHLY...
    tema_eu         TEXT,                       -- data-theme: EDUC, GOVE, ECON...
    paraules_clau   TEXT[],
    cobertura_temporal_inici DATE,
    cobertura_temporal_fi    DATE,
    data_creacio    DATE         NOT NULL DEFAULT CURRENT_DATE,
    data_modificacio DATE        NOT NULL DEFAULT CURRENT_DATE,
    -- Taula/vista dels marts de dbt que materialitza aquest dataset
    objecte_origen  TEXT
);

CREATE TABLE meta.distribucions (
    id_distribucio  SERIAL       PRIMARY KEY,
    id_dataset      TEXT         NOT NULL REFERENCES meta.datasets(id_dataset) ON DELETE CASCADE,
    format          TEXT         NOT NULL CHECK (format IN ('CSV','JSON','PARQUET','GEOJSON','XLSX')),
    url_acces       TEXT         NOT NULL,
    mida_bytes      BIGINT,
    data_modificacio DATE        NOT NULL DEFAULT CURRENT_DATE
);


-- ============================================================================
-- Usuari de lectura per a Power BI (mai el superusuari en un dashboard)
-- ============================================================================
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'powerbi_ro') THEN
        CREATE ROLE powerbi_ro LOGIN PASSWORD 'powerbi_local_dev';
    END IF;
END
$$;

GRANT USAGE ON SCHEMA ops, bronze, meta TO powerbi_ro;
GRANT SELECT ON ALL TABLES IN SCHEMA ops, bronze, meta TO powerbi_ro;
ALTER DEFAULT PRIVILEGES IN SCHEMA ops, bronze, meta
    GRANT SELECT ON TABLES TO powerbi_ro;
