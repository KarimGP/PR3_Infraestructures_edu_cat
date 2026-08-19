#!/usr/bin/env bash
# Ús: ./scripts/run_flink_job.sh 00_smoke_jdbc.py
#
# MSYS_NO_PATHCONV=1 és imprescindible a Git Bash sobre Windows:
# sense això, MinTTY converteix /opt/... en C:/Program Files/Git/opt/...
# La solució de la doble barra (//opt/...) tampoc serveix: PyFlink crea
# un enllaç simbòlic trencat cap a /job.py.
set -euo pipefail
JOB="${1:?Cal indicar el nom del job}"
MSYS_NO_PATHCONV=1 docker exec pr3-flink-jm \
    ./bin/flink run -py "/opt/flink_jobs/${JOB}"
