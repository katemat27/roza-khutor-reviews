#!/usr/bin/env bash
set -e

export AIRFLOW_HOME=/opt/airflow
export SUPERSET_CONFIG_PATH=/app/superset_config.py
export PATH="/opt/airflow-venv/bin:${PATH}"

PG_BIN=$(ls -d /usr/lib/postgresql/*/bin | head -1)
PGDATA=/var/lib/postgresql/data

mkdir -p "$PGDATA" /var/log/postgresql
chown -R postgres:postgres "$PGDATA" /var/log/postgresql

if [ ! -f "$PGDATA/PG_VERSION" ]; then
    su postgres -c "$PG_BIN/initdb -D $PGDATA"
fi

su postgres -c "$PG_BIN/pg_ctl -D $PGDATA -l /var/log/postgresql/postgres.log -w start"

until su postgres -c "$PG_BIN/pg_isready -q"; do
    sleep 1
done

if ! su postgres -c "$PG_BIN/psql -tAc \"SELECT 1 FROM pg_roles WHERE rolname='${POSTGRES_USER}'\"" | grep -q 1; then
    su postgres -c "$PG_BIN/psql -c \"CREATE ROLE ${POSTGRES_USER} LOGIN PASSWORD '${POSTGRES_PASSWORD}'\""
fi

if ! su postgres -c "$PG_BIN/psql -tAc \"SELECT 1 FROM pg_database WHERE datname='${POSTGRES_DB}'\"" | grep -q 1; then
    su postgres -c "$PG_BIN/createdb -O ${POSTGRES_USER} ${POSTGRES_DB}"
fi

/opt/airflow-venv/bin/airflow db migrate

/opt/airflow-venv/bin/airflow users create \
    --username "${AIRFLOW_ADMIN_USER}" \
    --firstname Admin \
    --lastname Admin \
    --email "${AIRFLOW_ADMIN_EMAIL}" \
    --role Admin \
    --password "${AIRFLOW_ADMIN_PASSWORD}" || true

superset db upgrade

superset fab create-admin \
    --username "${SUPERSET_ADMIN_USER}" \
    --firstname Admin \
    --lastname Admin \
    --email "${SUPERSET_ADMIN_EMAIL}" \
    --password "${SUPERSET_ADMIN_PASSWORD}" || true

superset init

exec /usr/bin/supervisord -c /etc/supervisor/conf.d/supervisord.conf
