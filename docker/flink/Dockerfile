# Imatge Flink 1.18.1 amb PyFlink i els connectors Kafka + JDBC ja instal·lats.
#
# Per què una imatge pròpia i no la oficial?
# Perquè Flink NO porta cap connector de sèrie. Els JARs han d'estar
# físicament a /opt/flink/lib ABANS d'arrencar el clúster: si els
# afegeixes després, el classloader no els veurà i tindràs l'error
# "Could not find any factory for identifier 'kafka'".

FROM flink:1.18.1-scala_2.12-java11

# ── Python + PyFlink ────────────────────────────────────────────
# La imatge base de Flink no porta Python. L'afegim per poder
# executar jobs PyFlink dins del contenidor (així no depens de
# la versió de Python que tinguis a Windows).
RUN apt-get update -y && \
    apt-get install -y --no-install-recommends \
        python3 python3-pip python3-dev curl && \
    ln -sf /usr/bin/python3 /usr/bin/python && \
    rm -rf /var/lib/apt/lists/*

# --break-system-packages cal a Debian bookworm (PEP 668).
# El fallback cobreix imatges base amb pip antic.
RUN pip3 install --no-cache-dir --break-system-packages apache-flink==1.18.1 \
    || pip3 install --no-cache-dir apache-flink==1.18.1

# ── Connectors ──────────────────────────────────────────────────
# VERSIONS CONGELADES. La nomenclatura és <versió-connector>-<versió-flink>.
# Barrejar un connector de 1.17 amb Flink 1.18 falla de maneres molt
# poc obvies (NoSuchMethodError en runtime, no en arrencar).
ARG FLINK_V=1.18
ARG KAFKA_CONN_V=3.1.0
ARG JDBC_CONN_V=3.1.2
ARG PG_DRIVER_V=42.7.3
ARG MAVEN=https://repo.maven.apache.org/maven2

RUN set -eux; \
    cd /opt/flink/lib; \
    curl -fSL -O ${MAVEN}/org/apache/flink/flink-sql-connector-kafka/${KAFKA_CONN_V}-${FLINK_V}/flink-sql-connector-kafka-${KAFKA_CONN_V}-${FLINK_V}.jar; \
    curl -fSL -O ${MAVEN}/org/apache/flink/flink-connector-jdbc/${JDBC_CONN_V}-${FLINK_V}/flink-connector-jdbc-${JDBC_CONN_V}-${FLINK_V}.jar; \
    curl -fSL -O ${MAVEN}/org/postgresql/postgresql/${PG_DRIVER_V}/postgresql-${PG_DRIVER_V}.jar; \
    chown flink:flink *.jar

# Comprovació ràpida: si algun JAR no hi és, el build peta aquí
# i no tres hores després amb un error críptic de Flink.
RUN ls -la /opt/flink/lib/ | grep -E "kafka|jdbc|postgresql"

WORKDIR /opt/flink
