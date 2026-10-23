CREATE SCHEMA IF NOT EXISTS {{ params.schema }};

CREATE TABLE IF NOT EXISTS {{ params.schema }}.{{ params.table_reviews }} (
    id           BIGSERIAL PRIMARY KEY,
    date         DATE,
    rating_1_5   NUMERIC,
    review_text  TEXT,
    category     TEXT,
    source       TEXT,
    valid_rating BOOLEAN,
    rating_pct   NUMERIC,
    row_hash     TEXT NOT NULL UNIQUE,
    loaded_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS {{ params.schema }}.{{ params.table_category }} (
    id                        BIGSERIAL PRIMARY KEY,
    category                  TEXT,
    count_category            INTEGER,
    count_category_negative   INTEGER,
    avg_rating_category       NUMERIC,
    calculated_at             TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS {{ params.schema }}.{{ params.table_overall }} (
    id            BIGSERIAL PRIMARY KEY,
    csi           NUMERIC,
    negative_pct  NUMERIC,
    calculated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
