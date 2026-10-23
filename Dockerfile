ARG SUPERSET_VERSION=4.1.4
FROM apache/superset:${SUPERSET_VERSION}

USER root

ARG AIRFLOW_VERSION=3.3.2
ARG PYTHON_VERSION=3.10

RUN apt-get update \
    && apt-get install -y --no-install-recommends postgresql supervisor curl \
    && rm -rf /var/lib/apt/lists/*

RUN pip install --no-cache-dir psycopg2-binary

COPY requirements.txt /tmp/requirements.txt
RUN python -m venv /opt/airflow-venv \
    && /opt/airflow-venv/bin/pip install --no-cache-dir --upgrade pip \
    && /opt/airflow-venv/bin/pip install --no-cache-dir "apache-airflow==${AIRFLOW_VERSION}" -r /tmp/requirements.txt \
       --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"

COPY supervisord.conf /etc/supervisor/conf.d/supervisord.conf
COPY start.sh /start.sh
COPY superset/superset_config.py /app/superset_config.py
COPY superset/bootstrap_dashboard.py /app/bootstrap_dashboard.py
COPY dags /opt/airflow/dags
COPY sql /opt/airflow/sql
COPY data /opt/airflow/data

RUN chmod +x /start.sh && mkdir -p /opt/airflow/logs

ENV AIRFLOW_HOME=/opt/airflow
ENV AIRFLOW__CORE__DAGS_FOLDER=/opt/airflow/dags
ENV PATH="/opt/airflow-venv/bin:${PATH}"
ENV SUPERSET_CONFIG_PATH=/app/superset_config.py

EXPOSE 8080 8088

CMD ["/start.sh"]
