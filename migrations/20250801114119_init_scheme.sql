-- +goose Up
-- +goose StatementBegin
-- 1. Схема
CREATE SCHEMA IF NOT EXISTS cloud_template AUTHORIZATION postgres;

------------------------------------------------------------------
-- 2. Таблицы «nomenclature_topic» и «nomenclature_topic_item»
------------------------------------------------------------------
CREATE TABLE cloud_template.nomenclature_topic
(
    id           bigserial PRIMARY KEY,
    office_id    bigint                               NOT NULL,
    task_type    varchar(50)                          NOT NULL,
    queue_number integer                              NOT NULL,
    priority     integer            DEFAULT 0         NOT NULL,
    created_at   timestamptz        DEFAULT now()     NOT NULL,
    last_update  timestamptz        DEFAULT now()     NOT NULL
);
ALTER TABLE cloud_template.nomenclature_topic OWNER TO postgres;

CREATE TABLE cloud_template.nomenclature_topic_item
(
    task_id   bigint NOT NULL
        REFERENCES cloud_template.nomenclature_topic(id) ON DELETE CASCADE,
    nm_id     bigint NOT NULL,
    office_id bigint NOT NULL,
    PRIMARY KEY (task_id, nm_id, office_id)
);
ALTER TABLE cloud_template.nomenclature_topic_item OWNER TO postgres;

CREATE INDEX idx_nti_nm_office
    ON cloud_template.nomenclature_topic_item (nm_id, office_id);

------------------------------------------------------------------
-- 3. Таблица «command_topic»
------------------------------------------------------------------
CREATE TABLE cloud_template.command_topic
(
    id           bigserial PRIMARY KEY,
    payload      jsonb,
    type         varchar(50)                          NOT NULL,
    queue_number integer                              NOT NULL,
    priority     bigint,
    office_id    bigint,
    created_at   timestamptz        DEFAULT now()     NOT NULL
);
ALTER TABLE cloud_template.command_topic OWNER TO postgres;

------------------------------------------------------------------
-- 4. Таблица «authorized_client»
------------------------------------------------------------------
CREATE TABLE cloud_template.authorized_client
(
    id         bigserial PRIMARY KEY,
    token      varchar(512)           NOT NULL UNIQUE,
    client     varchar(50)            NOT NULL UNIQUE,
    created_at timestamptz            DEFAULT now() NOT NULL
);
ALTER TABLE cloud_template.authorized_client OWNER TO postgres;
-- +goose StatementEnd

------------------------------------------------------------------
-- +goose Down
-- +goose StatementBegin
-- Удаляем всю схему вместе с объектами
DROP SCHEMA IF EXISTS cloud_template CASCADE;
-- +goose StatementEnd
