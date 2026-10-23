# Роза Хутор: аналитика отзывов на Apache Airflow 3 + PostgreSQL

Локальный стенд Apache Airflow 3 в Docker Compose: DAG читает выгрузку отзывов из CSV,
чистит её, считает метрики (CSI, доля негатива, метрики по категориям) и инкрементально
пишет результат в PostgreSQL в отдельную схему.

Все параметры задаются переменными окружения (файл `.env`); в коде и конфигурации хардкода нет.

## Содержание

- [Архитектура](#архитектура)
- [Состав проекта](#состав-проекта)
- [Требования](#требования)
- [Быстрый старт](#быстрый-старт)
- [Конфигурация](#конфигурация)
- [DAG](#dag)
- [Модель данных](#модель-данных)
- [Make-команды](#make-команды)
- [Проверка результата](#проверка-результата)
- [Уведомления по SMTP](#уведомления-по-smtp)
- [Диагностика](#диагностика)
- [Остановка и очистка](#остановка-и-очистка)

## Архитектура

Всё работает в **одном контейнере** (один Dockerfile, один сервис `app`). Процессами управляет `supervisord`:

| Компонент | Назначение |
|---|---|
| PostgreSQL | СУБД: метаданные Airflow и схема результатов DAG (метаданные Superset — sqlite в томе) |
| Airflow (`airflow standalone`) | api-server + scheduler + dag-processor + triggerer в одном процессе, порт `${AIRFLOW_WEB_PORT}` |
| Apache Superset | BI-дашборд, порт `${SUPERSET_WEB_PORT}` |

Airflow ставится в отдельный venv (`/opt/airflow-venv`), Superset использует Python базового образа —
зависимости не конфликтуют. Исполнитель — `LocalExecutor`.

Поток данных:

```
data/reviews.csv
      |
      v
create_tables -> extract_reviews -> clean_reviews -> calculate_metrics -> load_results -> notify_success
   (SQL DDL)       (pandas)          (pandas)           (pandas)            (to_sql -> PostgreSQL)
```

`create_tables` — первая задача в графе: идемпотентно создаёт схему и таблицы, после чего
остальные задачи работают с уже существующей схемой.

## Состав проекта

```
.
├── dags/
│   └── dag_roza_khutor.py
├── sql/
│   └── init.sql
├── data/
│   └── reviews.csv
├── superset/
│   ├── superset_config.py
│   └── bootstrap_dashboard.py
├── start.sh
├── supervisord.conf
├── .env
├── .dockerignore
├── .gitignore
├── Dockerfile
├── docker-compose.yaml
├── Makefile
├── README.md
└── requirements.txt
```

`dags/`, `sql/`, `data/` копируются в образ при сборке (`/opt/airflow/...`); внешних bind-монтирований нет.

## Требования

- Docker Desktop или Docker Engine + Compose v2
- Свободный хост-порт под Airflow UI (`AIRFLOW_WEB_PORT`, по умолчанию `8080`)
- Свободный хост-порт под Superset (`SUPERSET_WEB_PORT`, по умолчанию `8088`)
- Для Gmail-уведомлений (опционально) — app-password

## Быстрый старт

```bash
make init
make up
make ps
```

`make up` требует наличия файла `.env` с переменными окружения.

Web UI: `http://localhost:${AIRFLOW_WEB_PORT}` (по умолчанию `http://localhost:8080`).
Логин и пароль: `${AIRFLOW_ADMIN_USER}` / `${AIRFLOW_ADMIN_PASSWORD}` (по умолчанию `admin` / `admin`).

Запустить DAG вручную:

```bash
make trigger
```

## Конфигурация

Все значения — в `.env`. Файл `.env` в git не попадает.

### Образ и UI

| Переменная | По умолчанию | Описание |
|---|---|---|
| `AIRFLOW_VERSION` | `3.3.2` | Версия Airflow (тег образа) |
| `PYTHON_VERSION` | `3.12` | Версия Python базового образа |
| `AIRFLOW_IMAGE_NAME` | `airflow-roza-khutor` | Имя собираемого образа |
| `AIRFLOW_WEB_PORT` | `8080` | Хост-порт Web UI |
| `AIRFLOW__CORE__EXECUTOR` | `LocalExecutor` | Исполнитель задач |
| `AIRFLOW__CORE__AUTH_MANAGER` | `...FabAuthManager` | Менеджер аутентификации |
| `AIRFLOW__CORE__FERNET_KEY` | сгенерированный | Шифрование коннекторов и переменных |
| `AIRFLOW__CORE__LOAD_EXAMPLES` | `false` | Не грузить примеры DAG |
| `AIRFLOW__CORE__DAGS_ARE_PAUSED_AT_CREATION` | `false` | Новые DAG сразу активны |
| `AIRFLOW__API_AUTH__JWT_SECRET` | `airflow_jwt_secret` | Секрет JWT для execution API |
| `AIRFLOW__API_AUTH__JWT_ISSUER` | `airflow` | Issuer JWT |
| `AIRFLOW_ADMIN_USER` | `admin` | Логин администратора UI |
| `AIRFLOW_ADMIN_PASSWORD` | `admin` | Пароль администратора UI |
| `AIRFLOW_ADMIN_EMAIL` | `admin@example.com` | Email администратора |

### PostgreSQL

| Переменная | По умолчанию | Описание |
|---|---|---|
| `POSTGRES_VERSION` | `16` | Версия образа PostgreSQL |
| `POSTGRES_USER` | `airflow` | Пользователь БД |
| `POSTGRES_PASSWORD` | `airflow` | Пароль БД |
| `POSTGRES_DB` | `airflow` | Имя базы |
| `POSTGRES_HOST` | `postgres` | Хост внутри docker-сети |
| `POSTGRES_PORT` | `5432` | Порт внутри docker-сети |
| `POSTGRES_HOST_PORT` | `5434` | Порт на хосте |

### Superset

| Переменная | По умолчанию | Описание |
|---|---|---|
| `SUPERSET_IMAGE_NAME` | `superset-roza-khutor` | Имя собираемого образа Superset |
| `SUPERSET_VERSION` | `4.1.4` | Версия Superset |
| `SUPERSET_WEB_PORT` | `8088` | Хост-порт Web UI Superset |
| `SUPERSET_SECRET_KEY` | сгенерированный | Секрет Superset |
| `SUPERSET_DATABASE_URI` | `sqlite:////app/superset_home/superset.db` | Метаданные Superset |
| `SUPERSET_ADMIN_USER` | `admin` | Логин администратора Superset |
| `SUPERSET_ADMIN_PASSWORD` | `admin` | Пароль администратора Superset |
| `SUPERSET_ADMIN_EMAIL` | `admin@example.com` | Email администратора Superset |

### DAG и данные

| Переменная | По умолчанию | Описание |
|---|---|---|
| `ROZA_DAG_ID` | `roza_khutor_reviews` | Идентификатор DAG |
| `ROZA_SCHEDULE` | `0 8 * * *` | Расписание (cron) |
| `ROZA_START_DATE` | `2026-10-01` | Дата старта DAG (ISO) |
| `ROZA_RETRIES` | `2` | Число ретраев задачи |
| `ROZA_RETRY_DELAY_MIN` | `5` | Задержка ретрая, минуты |
| `ROZA_SCHEMA` | `roza_khutor` | Схема с результатами |
| `ROZA_TABLE_REVIEWS` | `reviews_cleaned` | Таблица очищенных отзывов |
| `ROZA_TABLE_CATEGORY` | `category_metrics` | Таблица метрик по категориям |
| `ROZA_TABLE_OVERALL` | `overall_metrics` | Таблица общих метрик |
| `ROZA_CSV_PATH` | `/opt/airflow/data/reviews.csv` | Путь к входному CSV в контейнере |
| `ROZA_PG_CONN_ID` | `roza_postgres` | Airflow-коннектор к PostgreSQL |
| `ROZA_SMTP_CONN_ID` | `smtp_gmail` | Airflow-коннектор SMTP |
| `ROZA_NOTIFY_EMAIL` | `kate.matusewitsch@gmail.com` | Получатель уведомления |
| `ROZA_SQL_DIR` | `/opt/airflow/sql` | Каталог с SQL-файлами (`template_searchpath`) |
| `ROZA_SQL_FILE` | `init.sql` | Имя SQL-файла с DDL |

Коннектор `roza_postgres` и строка подключения метаданных собираются в `docker-compose.yaml`
из переменных `POSTGRES_*`.

## DAG

Файл: `dags/dag_roza_khutor.py`.

| Задача | Тип | Что делает |
|---|---|---|
| `create_tables` | `SQLExecuteQueryOperator` | Выполняет `sql/init.sql`: создаёт схему и таблицы (идемпотентно) |
| `extract_reviews` | `PythonOperator` | Читает CSV и отдаёт данные в XCom (JSON) |
| `clean_reviews` | `PythonOperator` | Чистит данные: strip, дедупликация, валидные рейтинги 1–5, даты, `rating_pct` |
| `calculate_metrics` | `PythonOperator` | Считает `csi`, `negative_pct` и метрики по категориям |
| `load_results` | `PythonOperator` | Инкрементальная запись в PostgreSQL через `pandas.to_sql` |
| `notify_success` | `PythonOperator` | Отправляет письмо через `SmtpHook` |

Особенности:

- Хардкода нет: пути, схема, таблицы, коннекторы, email, расписание и ретраи берутся из env.
- DDL лежит отдельно (`sql/init.sql`) и выполняется SQL-оператором через `template_searchpath`;
  схема и имена таблиц подставляются Jinja-параметрами, поэтому и в SQL нет хардкода.
- `create_tables` стоит первой: к моменту записи таблицы уже существуют.
- Комментариев в коде и конфигурации нет.

## Модель данных

Схема — `ROZA_SCHEMA` (по умолчанию `roza_khutor`), база — `POSTGRES_DB`.

### reviews_cleaned

Очищенные отзывы. Append-only, дедупликация по `row_hash`.

| Колонка | Тип | Описание |
|---|---|---|
| `id` | `BIGSERIAL PK` | Суррогатный ключ |
| `date` | `DATE` | Дата отзыва |
| `rating_1_5` | `NUMERIC` | Оценка 1–5 |
| `review_text` | `TEXT` | Текст отзыва |
| `category` | `TEXT` | Категория |
| `source` | `TEXT` | Источник |
| `valid_rating` | `BOOLEAN` | Признак валидной оценки |
| `rating_pct` | `NUMERIC` | Оценка в процентах |
| `row_hash` | `TEXT UNIQUE` | md5 по бизнес-полям (ключ дедупликации) |
| `loaded_at` | `TIMESTAMPTZ` | Время загрузки (default `now()`) |

Инкремент: перед записью читаются существующие `row_hash`, новые строки догружаются через
`to_sql(if_exists='append')`. Повторный запуск не создаёт дублей.

### category_metrics

Метрики по категориям (история снапшотов, каждый прогон добавляет строки).

| Колонка | Тип | Описание |
|---|---|---|
| `id` | `BIGSERIAL PK` | Суррогатный ключ |
| `category` | `TEXT` | Категория |
| `count_category` | `INTEGER` | Число отзывов |
| `count_category_negative` | `INTEGER` | Число негативных отзывов (рейтинг <= 3) |
| `avg_rating_category` | `NUMERIC` | Средняя оценка |
| `calculated_at` | `TIMESTAMPTZ` | Время расчёта (default `now()`) |

### overall_metrics

Общие метрики (история снапшотов, каждый прогон добавляет строку).

| Колонка | Тип | Описание |
|---|---|---|
| `id` | `BIGSERIAL PK` | Суррогатный ключ |
| `csi` | `NUMERIC` | Customer Satisfaction Index (avg_rating x 2) |
| `negative_pct` | `NUMERIC` | Доля негативных отзывов, % |
| `calculated_at` | `TIMESTAMPTZ` | Время расчёта (default `now()`) |

## Make-команды

| Команда | Действие |
|---|---|
| `make init` | Проверить наличие `.env` |
| `make up` | Собрать образ и поднять стек |
| `make build` | Только собрать образ |
| `make down` | Остановить стек (тома сохраняются) |
| `make ps` | Статус контейнеров |
| `make logs` | Логи в реальном времени |
| `make trigger` | Запустить DAG вручную |
| `make dashboard` | Пересоздать дашборд Superset (бутстрап) |
| `make psql` | `psql` внутри контейнера `app` |
| `make clean` | Остановить и удалить контейнеры и тома |

## Проверка результата

```bash
make ps
```

Контейнер `app` должен быть `Up (healthy)`.

Список DAG и ошибки импорта:

```bash
docker compose exec app airflow dags list
docker compose exec app airflow dags list-import-errors
```

Запуск и статусы:

```bash
make trigger
docker compose exec app airflow dags list-runs "${ROZA_DAG_ID}"
docker compose exec app airflow tasks states-for-dag-run "${ROZA_DAG_ID}" '<run_id>'
```

Данные в PostgreSQL:

```bash
docker compose exec app /usr/lib/postgresql/15/bin/psql "postgresql://${POSTGRES_USER}:${POSTGRES_PASSWORD}@localhost:5432/${POSTGRES_DB}" -c "select count(*) from ${ROZA_SCHEMA}.${ROZA_TABLE_REVIEWS};"
docker compose exec app /usr/lib/postgresql/15/bin/psql "postgresql://${POSTGRES_USER}:${POSTGRES_PASSWORD}@localhost:5432/${POSTGRES_DB}" -c "select * from ${ROZA_SCHEMA}.${ROZA_TABLE_OVERALL} order by id;"
```

Таблицы по схемам:

```bash
docker compose exec app /usr/lib/postgresql/15/bin/psql "postgresql://${POSTGRES_USER}:${POSTGRES_PASSWORD}@localhost:5432/${POSTGRES_DB}" -c "\dt ${ROZA_SCHEMA}.*"
```

PostgreSQL наружу не публикуется (он внутри контейнера). Доступ к данным — через `make psql`
или через Superset.

## Superset (дашборд)

Superset работает в том же контейнере (порт `${SUPERSET_WEB_PORT}`). Метаданные Superset —
sqlite в томе `superset_home`; данные берутся из той же PostgreSQL (БД `${POSTGRES_DB}`),
схема `${ROZA_SCHEMA}`.

- Web UI: `http://localhost:${SUPERSET_WEB_PORT}` (по умолчанию `http://localhost:8088`)
- Логин/пароль: `${SUPERSET_ADMIN_USER}` / `${SUPERSET_ADMIN_PASSWORD}` (по умолчанию `admin` / `admin`)

При старте контейнера `start.sh` выполняет `superset db upgrade`, создаёт администратора,
выполняет `superset init`; затем `bootstrap_dashboard.py` (через `supervisord`) идемпотентно создаёт:

- подключение к базе `roza_khutor`;
- датасеты по таблицам `${ROZA_SCHEMA}.${ROZA_TABLE_REVIEWS}`, `${ROZA_TABLE_CATEGORY}`, `${ROZA_TABLE_OVERALL}`;
- графики: CSI, «Доля негатива» и таблицу «Метрики по категориям»;
- дашборд «Роза Хутор: отзывы» (slug `roza-khutor`).

Пересоздать дашборд вручную:

```bash
make dashboard
```

## Уведомления по SMTP

Задача `notify_success` использует коннектор `${ROZA_SMTP_CONN_ID}`. Пока коннектор не задан,
задача падает с `The conn_id ... isn't defined`; на загрузку данных это не влияет
(`load_results` выполняется раньше).

Вариант 1 — через `.env` (передаётся в контейнеры через `env_file`):

```
AIRFLOW_CONN_SMTP_GMAIL={"conn_type": "smtp", "host": "smtp.gmail.com", "port": 587, "login": "you@gmail.com", "password": "app-password"}
```

После изменения `.env`: `make down && make up`.

Вариант 2 — через UI: Admin -> Connections -> +, Conn Id = `${ROZA_SMTP_CONN_ID}`, Type = `SMTP`.

Если письмо не нужно — игнорируйте падение задачи после ретраев либо пометьте её успешной в UI
(DAG Run -> `notify_success` -> Mark Success).

## Диагностика

- **Порт занят.** Поменяйте `POSTGRES_HOST_PORT` / `AIRFLOW_WEB_PORT` в `.env` и выполните `make down && make up`.
- **Ошибки импорта DAG.** `docker compose exec airflow-apiserver airflow dags list-import-errors`;
  чаще всего не задана переменная окружения.
- **Монтирования.** `./dags`, `./sql`, `./data` монтируются read-only в `/opt/airflow/dags`,
  `/opt/airflow/sql`, `/opt/airflow/data`.
- **Логи сервиса.** `docker compose logs -f airflow-scheduler` (или другой сервис).

## Остановка и очистка

```bash
make down
make clean
```

`make down` останавливает стек с сохранением томов и образа. `make clean` удаляет контейнеры
и тома (включая `postgres-db-volume`). Файл `data/reviews.csv` лежит на хосте и не удаляется.



