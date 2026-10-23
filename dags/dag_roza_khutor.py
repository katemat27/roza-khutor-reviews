import hashlib
import os
from datetime import timedelta
from io import StringIO

import pandas as pd
import pendulum
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.smtp.hooks.smtp import SmtpHook
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG

DAG_ID = os.environ['ROZA_DAG_ID']
SCHEDULE = os.environ['ROZA_SCHEDULE']
START_DATE = os.environ['ROZA_START_DATE']
RETRIES = int(os.environ['ROZA_RETRIES'])
RETRY_DELAY_MIN = int(os.environ['ROZA_RETRY_DELAY_MIN'])
SCHEMA = os.environ['ROZA_SCHEMA']
TABLE_REVIEWS = os.environ['ROZA_TABLE_REVIEWS']
TABLE_CATEGORY = os.environ['ROZA_TABLE_CATEGORY']
TABLE_OVERALL = os.environ['ROZA_TABLE_OVERALL']
CSV_PATH = os.environ['ROZA_CSV_PATH']
PG_CONN_ID = os.environ['ROZA_PG_CONN_ID']
SMTP_CONN_ID = os.environ['ROZA_SMTP_CONN_ID']
NOTIFY_EMAIL = os.environ['ROZA_NOTIFY_EMAIL']
SQL_DIR = os.environ['ROZA_SQL_DIR']
SQL_FILE = os.environ['ROZA_SQL_FILE']

SQL_PARAMS = {
    'schema': SCHEMA,
    'table_reviews': TABLE_REVIEWS,
    'table_category': TABLE_CATEGORY,
    'table_overall': TABLE_OVERALL,
}


def extract_reviews(file_path):
    try:
        data = pd.read_csv(file_path)
    except FileNotFoundError as error:
        raise FileNotFoundError(f'Не найден CSV-файл с отзывами: {file_path}') from error
    return data.to_json(orient='split', date_format='iso')


def clean_reviews(data):
    cleaned = data.copy()
    required_columns = {'date', 'rating_1_5', 'review_text', 'category', 'source'}
    for column in required_columns:
        if column not in data.columns:
            raise ValueError(f'В файле отсутствует обязательный столбец: {column}')
    for column in ['category', 'source', 'review_text']:
        cleaned[column] = cleaned[column].str.strip()
    cleaned = cleaned.drop_duplicates()
    cleaned['rating_1_5'] = pd.to_numeric(cleaned['rating_1_5'], errors='coerce')
    cleaned['valid_rating'] = cleaned['rating_1_5'].between(1, 5)
    cleaned = cleaned[cleaned['valid_rating'] == True]
    cleaned['category'] = cleaned['category'].fillna('Не указана')
    cleaned['date'] = pd.to_datetime(cleaned['date'], errors='coerce')
    cleaned['rating_pct'] = (cleaned['rating_1_5'] - 1) / 4 * 100
    return cleaned


def calculate_metrics(cleaned):
    if cleaned.empty:
        raise ValueError('После очистки не осталось отзывов для расчёта метрик')
    count_category = cleaned.groupby(by=['category'])['rating_1_5'].count()
    count_category_negative = cleaned[cleaned['rating_1_5'] <= 3].groupby(by=['category'])['rating_1_5'].count()
    count_category_negative = count_category_negative.reindex(count_category.index, fill_value=0)
    csi = cleaned['rating_1_5'].mean() * 2
    negative_pct = (count_category_negative.sum() / len(cleaned)) * 100
    avg_rating_category = cleaned.groupby(by=['category'])['rating_1_5'].mean()
    overall_metrics = pd.DataFrame([{'csi': csi, 'negative_pct': negative_pct}])
    overall_metrics_category = pd.DataFrame({
        'count_category': count_category,
        'count_category_negative': count_category_negative,
        'avg_rating_category': avg_rating_category,
    })
    overall_metrics = overall_metrics.round(2)
    overall_metrics_category = overall_metrics_category.round(2)
    overall_metrics_category = overall_metrics_category.reset_index()
    return overall_metrics, overall_metrics_category


def load_results(cleaned, overall_metrics, category_metrics):
    hook = PostgresHook(postgres_conn_id=PG_CONN_ID)
    engine = hook.get_sqlalchemy_engine()
    try:
        cleaned = cleaned.copy()
        cleaned['row_hash'] = (
            cleaned['date'].astype(str) + '|'
            + cleaned['rating_1_5'].astype(str) + '|'
            + cleaned['review_text'].astype(str) + '|'
            + cleaned['category'].astype(str) + '|'
            + cleaned['source'].astype(str)
        ).apply(lambda value: hashlib.md5(value.encode('utf-8')).hexdigest())

        existing = pd.read_sql(
            f'SELECT row_hash FROM {SCHEMA}.{TABLE_REVIEWS}', engine
        )['row_hash'].tolist()
        new_reviews = cleaned[~cleaned['row_hash'].isin(existing)]
        new_reviews[[
            'date', 'rating_1_5', 'review_text', 'category', 'source',
            'valid_rating', 'rating_pct', 'row_hash',
        ]].to_sql(TABLE_REVIEWS, engine, schema=SCHEMA, if_exists='append', index=False)

        category_metrics.to_sql(TABLE_CATEGORY, engine, schema=SCHEMA, if_exists='append', index=False)
        overall_metrics.to_sql(TABLE_OVERALL, engine, schema=SCHEMA, if_exists='append', index=False)
    finally:
        engine.dispose()


def clean_reviews_task(ti):
    reviews_json = ti.xcom_pull(task_ids='extract_reviews')
    data = pd.read_json(StringIO(reviews_json), orient='split')
    cleaned = clean_reviews(data)
    return cleaned.to_json(orient='split', date_format='iso')


def calculate_metrics_task(ti):
    cleaned_json = ti.xcom_pull(task_ids='clean_reviews')
    cleaned = pd.read_json(StringIO(cleaned_json), orient='split')
    overall, categories = calculate_metrics(cleaned)
    return {
        'overall': overall.to_json(orient='split'),
        'categories': categories.to_json(orient='split'),
    }


def load_results_task(ti):
    cleaned_json = ti.xcom_pull(task_ids='clean_reviews')
    metrics = ti.xcom_pull(task_ids='calculate_metrics')
    cleaned = pd.read_json(StringIO(cleaned_json), orient='split')
    cleaned['date'] = pd.to_datetime(cleaned['date'], errors='coerce')
    overall = pd.read_json(StringIO(metrics['overall']), orient='split')
    categories = pd.read_json(StringIO(metrics['categories']), orient='split')
    load_results(cleaned, overall, categories)


def notify_success(recipient):
    with SmtpHook(smtp_conn_id=SMTP_CONN_ID) as hook:
        hook.send_email_smtp(
            to=recipient,
            subject='Обработка отзывов Роза Хутор завершена',
            html_content='<p>Отзывы обработаны, метрики рассчитаны и сохранены в базе.</p>',
        )


default_args = {
    'retries': RETRIES,
    'retry_delay': timedelta(minutes=RETRY_DELAY_MIN),
}

with DAG(
    dag_id=DAG_ID,
    start_date=pendulum.parse(START_DATE),
    schedule=SCHEDULE,
    catchup=False,
    default_args=default_args,
    max_active_runs=1,
    template_searchpath=[SQL_DIR],
) as dag:
    create_tables_task = SQLExecuteQueryOperator(
        task_id='create_tables',
        conn_id=PG_CONN_ID,
        sql=SQL_FILE,
        params=SQL_PARAMS,
        split_statements=True,
    )

    extract_task = PythonOperator(
        task_id='extract_reviews',
        python_callable=extract_reviews,
        op_kwargs={'file_path': CSV_PATH},
    )

    clean_task = PythonOperator(
        task_id='clean_reviews',
        python_callable=clean_reviews_task,
    )

    metrics_task = PythonOperator(
        task_id='calculate_metrics',
        python_callable=calculate_metrics_task,
    )

    load_task = PythonOperator(
        task_id='load_results',
        python_callable=load_results_task,
    )

    notify_task = PythonOperator(
        task_id='notify_success',
        python_callable=notify_success,
        op_kwargs={'recipient': NOTIFY_EMAIL},
    )

    create_tables_task >> extract_task >> clean_task >> metrics_task >> load_task >> notify_task

