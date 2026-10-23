import json
import os
import sqlite3
import subprocess
import time

import psycopg2
import requests

BASE = 'http://localhost:8088'
ADMIN_USER = os.environ['SUPERSET_ADMIN_USER']
ADMIN_PASSWORD = os.environ['SUPERSET_ADMIN_PASSWORD']
SCHEMA = os.environ['ROZA_SCHEMA']
TABLE_REVIEWS = os.environ['ROZA_TABLE_REVIEWS']
TABLE_CATEGORY = os.environ['ROZA_TABLE_CATEGORY']
TABLE_OVERALL = os.environ['ROZA_TABLE_OVERALL']
DB_NAME = 'roza_khutor'
DB_URI = (
    f"postgresql://{os.environ['POSTGRES_USER']}:{os.environ['POSTGRES_PASSWORD']}"
    f"@{os.environ['POSTGRES_HOST']}:{os.environ['POSTGRES_PORT']}/{os.environ['POSTGRES_DB']}"
)
DASHBOARD_TITLE = 'Роза Хутор: отзывы'
SQL_PATH = os.environ.get('ROZA_SQL_PATH', '/opt/airflow/sql/init.sql')


def ensure_tables():
    sql = open(SQL_PATH, encoding='utf-8').read()
    for key, value in {
        'schema': SCHEMA,
        'table_reviews': TABLE_REVIEWS,
        'table_category': TABLE_CATEGORY,
        'table_overall': TABLE_OVERALL,
    }.items():
        sql = sql.replace('{{ params.' + key + ' }}', value)
    connection = psycopg2.connect(
        host=os.environ['POSTGRES_HOST'],
        port=os.environ['POSTGRES_PORT'],
        dbname=os.environ['POSTGRES_DB'],
        user=os.environ['POSTGRES_USER'],
        password=os.environ['POSTGRES_PASSWORD'],
    )
    connection.autocommit = True
    with connection.cursor() as cursor:
        cursor.execute(sql)
    connection.close()


def ensure_database(session):
    response = session.get(f'{BASE}/api/v1/database/', timeout=30)
    for item in response.json()['result']:
        if item['database_name'] == DB_NAME:
            return item['id']
    response = session.post(
        f'{BASE}/api/v1/database/',
        json={'database_name': DB_NAME, 'sqlalchemy_uri': DB_URI, 'expose_in_sqllab': True},
        timeout=30,
    )
    response.raise_for_status()
    return response.json()['id']


def ensure_dataset(session, db_id, table_name):
    response = session.get(f'{BASE}/api/v1/dataset/?q=(page_size:100)', timeout=30)
    for item in response.json()['result']:
        if item['table_name'] == table_name and item.get('schema') == SCHEMA:
            return item['id']
    response = session.post(
        f'{BASE}/api/v1/dataset/',
        json={'database': db_id, 'schema': SCHEMA, 'table_name': table_name},
        timeout=30,
    )
    response.raise_for_status()
    data = response.json()
    return data.get('id') or data['result']['id']


def ensure_chart(session, name, viz_type, dataset_id, params):
    payload = {
        'slice_name': name,
        'viz_type': viz_type,
        'datasource_id': dataset_id,
        'datasource_type': 'table',
        'params': json.dumps(params),
    }
    response = session.get(f'{BASE}/api/v1/chart/?q=(page_size:200)', timeout=30)
    for item in response.json()['result']:
        if item['slice_name'] == name:
            session.put(f"{BASE}/api/v1/chart/{item['id']}", json=payload, timeout=30).raise_for_status()
            return item['id']
    response = session.post(f'{BASE}/api/v1/chart/', json=payload, timeout=30)
    response.raise_for_status()
    data = response.json()
    return data.get('id') or data['result']['id']


def link_charts(dashboard_id, chart_ids):
    path = os.environ['SUPERSET_DATABASE_URI'].replace('sqlite:///', '')
    connection = sqlite3.connect(path, timeout=30)
    try:
        for chart_id in chart_ids:
            connection.execute(
                'INSERT OR IGNORE INTO dashboard_slices (dashboard_id, slice_id) VALUES (?, ?)',
                (dashboard_id, chart_id),
            )
        connection.commit()
    finally:
        connection.close()


def restart_superset():
    subprocess.run(['supervisorctl', 'restart', 'superset'], check=False)
    for _ in range(60):
        try:
            if requests.get(f'{BASE}/health', timeout=3).status_code == 200:
                return
        except requests.RequestException:
            pass
        time.sleep(2)


def ensure_dashboard(session, chart_ids):
    response = session.get(f'{BASE}/api/v1/dashboard/?q=(page_size:100)', timeout=30)
    dashboard_id = None
    for item in response.json()['result']:
        if item['dashboard_title'] == DASHBOARD_TITLE:
            dashboard_id = item['id']
            break
    if dashboard_id is None:
        response = session.post(
            f'{BASE}/api/v1/dashboard/',
            json={'dashboard_title': DASHBOARD_TITLE, 'slug': 'roza-khutor', 'published': True},
            timeout=30,
        )
        response.raise_for_status()
        data = response.json()
        dashboard_id = data.get('id') or data['result']['id']

    row_id = 'ROW-1'
    layout = {
        'DASHBOARD_VERSION_KEY': 'v2',
        'ROOT_ID': {'type': 'ROOT', 'id': 'ROOT_ID', 'children': ['GRID_ID']},
        'GRID_ID': {'type': 'GRID', 'id': 'GRID_ID', 'children': [row_id], 'parents': ['ROOT_ID']},
        row_id: {
            'type': 'ROW',
            'id': row_id,
            'children': [],
            'parents': ['ROOT_ID', 'GRID_ID'],
            'meta': {'background': 'BACKGROUND_TRANSPARENT'},
        },
    }
    for index, chart_id in enumerate(chart_ids, start=1):
        chart_key = f'CHART-{index}'
        layout[row_id]['children'].append(chart_key)
        layout[chart_key] = {
            'type': 'CHART',
            'id': chart_key,
            'children': [],
            'parents': ['ROOT_ID', 'GRID_ID', row_id],
            'meta': {'width': 4, 'height': 50, 'chartId': chart_id},
        }

    response = session.put(
        f'{BASE}/api/v1/dashboard/{dashboard_id}',
        json={
            'published': True,
            'position_json': json.dumps(layout),
            'json_metadata': json.dumps({
                'chart_configuration': {},
                'global_chart_configuration': {
                    'scope': {'rootPath': ['ROOT_ID'], 'excluded': []},
                    'globalChartConfigById': {},
                },
                'timed_refresh_immune_slices': [],
                'expanded_slices': {},
                'refresh_frequency': 0,
                'default_filters': '{}',
            }),
        },
        timeout=30,
    )
    response.raise_for_status()
    link_charts(dashboard_id, chart_ids)
    print(f'dashboard id={dashboard_id}')


def main():
    session = requests.Session()
    token = session.post(
        f'{BASE}/api/v1/security/login',
        json={'username': ADMIN_USER, 'password': ADMIN_PASSWORD, 'provider': 'db', 'refresh': True},
        timeout=30,
    ).json()['access_token']
    session.headers.update({'Authorization': f'Bearer {token}'})

    ensure_tables()
    db_id = ensure_database(session)
    dataset_overall = ensure_dataset(session, db_id, TABLE_OVERALL)
    dataset_category = ensure_dataset(session, db_id, TABLE_CATEGORY)
    ensure_dataset(session, db_id, TABLE_REVIEWS)

    csi_chart = ensure_chart(session, 'CSI', 'big_number_total', dataset_overall, {
        'viz_type': 'big_number_total',
        'metric': {
            'expressionType': 'SIMPLE',
            'column': {'column_name': 'csi'},
            'aggregate': 'AVG',
            'label': 'CSI',
        },
        'subheader': 'CSI',
    })
    negative_chart = ensure_chart(session, 'Доля негатива', 'big_number_total', dataset_overall, {
        'viz_type': 'big_number_total',
        'metric': {
            'expressionType': 'SIMPLE',
            'column': {'column_name': 'negative_pct'},
            'aggregate': 'AVG',
            'label': 'Negative %',
        },
        'subheader': 'Негатив, %',
    })
    category_chart = ensure_chart(session, 'Метрики по категориям', 'table', dataset_category, {
        'viz_type': 'table',
        'query_mode': 'aggregate',
        'groupby': ['category'],
        'metrics': [
            {'expressionType': 'SIMPLE', 'column': {'column_name': 'count_category'}, 'aggregate': 'AVG', 'label': 'Отзывов'},
            {'expressionType': 'SIMPLE', 'column': {'column_name': 'count_category_negative'}, 'aggregate': 'AVG', 'label': 'Негативных'},
            {'expressionType': 'SIMPLE', 'column': {'column_name': 'avg_rating_category'}, 'aggregate': 'AVG', 'label': 'Средняя оценка'},
        ],
        'row_limit': 100,
    })

    ensure_dashboard(session, [csi_chart, negative_chart, category_chart])
    restart_superset()
    print('bootstrap done')


if __name__ == '__main__':
    main()

