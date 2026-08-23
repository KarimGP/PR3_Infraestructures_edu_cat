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
    codi_centre     VARCHAR(8)   PRIMARY KEY,   -- codi oficial del Registre de Centres Docents
    denominacio     TEXT         NOT NULL,
    -- Naturalesa i titularitat venen codificades a la font. Les guardem
    -- tal com arriben (codi + nom) sense reinterpretar-les.
    codi_naturalesa VARCHAR(2),
    nom_naturalesa  TEXT,                       -- Public / Privat
    codi_titularitat VARCHAR(4),
    nom_titularitat TEXT,                       -- Departament d'Educacio, Cooperatives...
    -- Localitzacio
    codi_ine        VARCHAR(6)   NOT NULL REFERENCES ops.municipis(codi_ine),
    adreca          TEXT,
    codi_postal     VARCHAR(5),
    latitud         NUMERIC(9,6),
    longitud        NUMERIC(9,6),
    -- Ensenyaments autoritzats. La font els dona com a banderes
    -- independents, no com una categoria unica: un mateix centre pot
    -- impartir infantil, primaria i ESO alhora. Guardar-ho aixi permet
    -- consultes que un sol camp "tipus" perdria. La classificacio en
    -- INS/CEIP/CFA es fa a la capa de dbt, on queda documentada i testada.
    te_infantil_1c  BOOLEAN      NOT NULL DEFAULT FALSE,
    te_infantil_2c  BOOLEAN      NOT NULL DEFAULT FALSE,
    te_primaria     BOOLEAN      NOT NULL DEFAULT FALSE,
    te_eso          BOOLEAN      NOT NULL DEFAULT FALSE,
    te_batxillerat  BOOLEAN      NOT NULL DEFAULT FALSE,
    te_fp_mitja     BOOLEAN      NOT NULL DEFAULT FALSE,
    te_fp_superior  BOOLEAN      NOT NULL DEFAULT FALSE,
    te_adults       BOOLEAN      NOT NULL DEFAULT FALSE,
    te_especial     BOOLEAN      NOT NULL DEFAULT FALSE,
    -- Atributs sintetics: no venen de la font, els generem nosaltres.
    -- Serviran de variables predictores al model del dia 13.
    any_construccio SMALLINT     CHECK (any_construccio BETWEEN 1850 AND 2030),
    superficie_m2   NUMERIC(10,2) CHECK (superficie_m2 > 0),
    num_alumnes     INTEGER      CHECK (num_alumnes >= 0),
    estat_conservacio SMALLINT   CHECK (estat_conservacio BETWEEN 0 AND 100),
    -- Traçabilitat de la font
    curs_font       VARCHAR(9),                 -- p.ex. 2025/2026
    actiu           BOOLEAN      NOT NULL DEFAULT TRUE,
    created_at      TIMESTAMPTZ  NOT NULL DEFAULT now(),
    updated_at      TIMESTAMPTZ  NOT NULL DEFAULT now()
);

CREATE INDEX idx_centres_municipi ON ops.centres(codi_ine);
CREATE INDEX idx_centres_eso      ON ops.centres(te_eso) WHERE te_eso;
CREATE INDEX idx_centres_primaria ON ops.centres(te_primaria) WHERE te_primaria;

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
-- Quin lot cobreix quin municipi. Els contractes marc de manteniment
-- es divideixen per agrupacions territorials de municipis (o per
-- districtes, a les ciutats grans), no per comarca: dins d'una sola
-- comarca hi pot haver diversos lots.
CREATE TABLE ops.lot_cobertura (
    lot             SMALLINT     NOT NULL CHECK (lot BETWEEN 1 AND 3),
    codi_ine        VARCHAR(6)   NOT NULL REFERENCES ops.municipis(codi_ine),
    PRIMARY KEY (lot, codi_ine)
);


-- ============================================================================
-- 4. INCIDÈNCIES I ACTUACIONS (el cor transaccional)
-- ============================================================================

CREATE TABLE ops.tipus_incidencia (
    codi_tipus      VARCHAR(10)  PRIMARY KEY,
    familia         TEXT         NOT NULL
                                 CHECK (familia IN ('CLIMATITZACIO','FONTANERIA','FUSTERIA',
                                                    'PALETERIA','ELECTRICITAT','ESTRUCTURA','ALTRES')),
    descripcio      TEXT         NOT NULL,
    -- Probabilitats que aquest tipus dispari cadascun dels dos criteris
    -- d'urgencia. No son atributs fixos: una mateixa averia pot
    -- interrompre l'activitat o no segons on i quan passi.
    prob_seguretat  NUMERIC(3,2) NOT NULL DEFAULT 0 CHECK (prob_seguretat BETWEEN 0 AND 1),
    prob_interrupcio NUMERIC(3,2) NOT NULL DEFAULT 0 CHECK (prob_interrupcio BETWEEN 0 AND 1),
    -- Pes relatiu d'aquest tipus en el total d'incidencies. Serveix
    -- al generador per reproduir la distribucio real per families.
    pes_relatiu     NUMERIC(5,4) NOT NULL CHECK (pes_relatiu > 0),
    -- Cost tipic d'una actuacio d'aquesta mena, en euros.
    cost_mitja      NUMERIC(10,2) NOT NULL CHECK (cost_mitja >= 0)
);

-- Els SLA no depenen del tipus sino de l'impacte, que es com es
-- prioritza realment al manteniment educatiu: si afecta la seguretat
-- o interromp l'activitat docent, 24h encara que sigui provisional;
-- la resta, 5 dies.
CREATE TABLE ops.sla (
    codi_sla        VARCHAR(10)  PRIMARY KEY,
    descripcio      TEXT         NOT NULL,
    hores_maximes   INTEGER      NOT NULL CHECK (hores_maximes > 0)
);

INSERT INTO ops.sla (codi_sla, descripcio, hores_maximes) VALUES
    ('URGENT',  'Afecta seguretat o interromp activitat docent', 24),
    ('NORMAL',  'Resta d''incidencies',                         120);

CREATE TABLE ops.incidencies (
    id_incidencia   BIGSERIAL    PRIMARY KEY,
    uuid_origen     UUID         UNIQUE,
    codi_centre     VARCHAR(8)   NOT NULL REFERENCES ops.centres(codi_centre),
    codi_tipus      VARCHAR(10)  NOT NULL REFERENCES ops.tipus_incidencia(codi_tipus),
    -- La prioritat no es un atribut lliure: es deriva de si la
    -- incidencia afecta la seguretat o interromp l'activitat docent.
    -- Aixi es com es prioritza realment al manteniment educatiu.
    requereix_seguretat BOOLEAN  NOT NULL DEFAULT FALSE,
    interromp_activitat BOOLEAN  NOT NULL DEFAULT FALSE,
    estat           TEXT         NOT NULL DEFAULT 'OBERTA'
                                 CHECK (estat IN ('OBERTA','ASSIGNADA','EN_CURS',
                                                  'RESOLTA_PROVISIONAL','RESOLTA','TANCADA','ANULADA')),
    descripcio      TEXT,
    canal_entrada   TEXT         CHECK (canal_entrada IN ('TELEFON','WEB','SENSOR','INSPECCIO')),
    data_obertura   TIMESTAMPTZ  NOT NULL,
    data_assignacio TIMESTAMPTZ,
    -- Dues dates de resolucio, no una. Una avaria de calefaccio al
    -- gener es tapa en 24h amb una solucio temporal i es resol de
    -- veritat setmanes despres. Barrejar-les distorsionaria el
    -- compliment de SLA, que es mesura contra la provisional.
    data_resolucio_provisional TIMESTAMPTZ,
    data_resolucio_definitiva  TIMESTAMPTZ,
    cost_estimat    NUMERIC(12,2) CHECK (cost_estimat >= 0),
    created_at      TIMESTAMPTZ  NOT NULL DEFAULT now(),
    updated_at      TIMESTAMPTZ  NOT NULL DEFAULT now(),
    CONSTRAINT chk_inc_prov_posterior
        CHECK (data_resolucio_provisional IS NULL
               OR data_resolucio_provisional >= data_obertura),
    CONSTRAINT chk_inc_def_posterior
        CHECK (data_resolucio_definitiva IS NULL
               OR data_resolucio_definitiva >= data_obertura),
    -- Si hi ha les dues, la definitiva mai pot precedir la provisional
    CONSTRAINT chk_inc_ordre_resolucions
        CHECK (data_resolucio_provisional IS NULL
               OR data_resolucio_definitiva IS NULL
               OR data_resolucio_definitiva >= data_resolucio_provisional),
    CONSTRAINT chk_inc_resolta
        CHECK (estat NOT IN ('RESOLTA','TANCADA')
               OR data_resolucio_definitiva IS NOT NULL)
);

CREATE INDEX idx_inc_centre   ON ops.incidencies(codi_centre);
CREATE INDEX idx_inc_obertura ON ops.incidencies(data_obertura);
CREATE INDEX idx_inc_estat    ON ops.incidencies(estat) WHERE estat NOT IN ('TANCADA','ANULADA');
CREATE INDEX idx_inc_tipus    ON ops.incidencies(codi_tipus);
CREATE INDEX idx_inc_urgents  ON ops.incidencies(data_obertura)
    WHERE requereix_seguretat OR interromp_activitat;

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
