-- ============================================================================
-- Taula de prova de foc per validar el sink JDBC de Flink el dia 1.
--
-- Deliberadament trivial: dues columnes, cap constraint, cap dependència.
-- Si Flink hi pot escriure, el camí Flink -> JDBC -> PostgreSQL està validat
-- i qualsevol error posterior serà de lògica, no d'infraestructura.
--
-- Es pot eliminar abans de la publicació final, però val la pena deixar-la:
-- documenta com has derisquitzat el projecte.
-- ============================================================================

CREATE TABLE IF NOT EXISTS bronze.smoke_test (
    id          INTEGER      PRIMARY KEY,
    missatge    TEXT,
    inserted_at TIMESTAMPTZ  NOT NULL DEFAULT now()
);

GRANT SELECT ON bronze.smoke_test TO powerbi_ro;
