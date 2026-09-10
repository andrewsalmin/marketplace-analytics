# На сервере стенда: /opt/superset/Dockerfile
# Собирается из docker-compose.yml (`build: .`), рядом.
#
# Образ Superset идёт без драйверов к нашим базам: clickhouse-connect
# нужен, чтобы дашборд вообще ходил в хранилище, psycopg2 — чтобы сам
# Superset хранил метаданные в postgres, а не в sqlite.

FROM apache/superset:latest

USER root

RUN uv pip install --python /app/.venv/bin/python --no-cache \
    clickhouse-connect \
    psycopg2-binary

USER superset
