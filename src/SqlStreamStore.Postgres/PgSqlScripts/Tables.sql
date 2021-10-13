CREATE SEQUENCE IF NOT EXISTS __schema__.streams_seq
  START 1;

CREATE TABLE IF NOT EXISTS __schema__.streams (
  id                   CHAR(42)      NOT NULL,
  id_original          VARCHAR(1000) NOT NULL,
  id_internal          INT           NOT NULL DEFAULT nextval('__schema__.streams_seq'),
  version              INT           NOT NULL DEFAULT (-1),
  position             BIGINT        NOT NULL DEFAULT (-1),
  max_age              INT           NULL,
  max_count            INT           NULL,
  CONSTRAINT pk_streams PRIMARY KEY (id_internal),
  CONSTRAINT uq_streams_id UNIQUE (id),
  CONSTRAINT ck_version_gte_negative_one CHECK (version >= -1)
);

CREATE INDEX IF NOT EXISTS ix_id_original
  ON __schema__.streams (id_original);

CREATE INDEX IF NOT EXISTS ix_id_original_reversed
  ON __schema__.streams (REVERSE(id_original));

COMMENT ON SCHEMA __schema__
IS '{ "version": 1 }';

ALTER SEQUENCE __schema__.streams_seq
  OWNED BY __schema__.streams.id_internal;

CREATE SEQUENCE IF NOT EXISTS __schema__.messages_seq
  START 0
  MINVALUE 0;

CREATE TABLE IF NOT EXISTS __schema__.messages (
  stream_id_internal INT          NOT NULL,
  stream_version     INT          NOT NULL,
  "position"         BIGINT       NOT NULL DEFAULT nextval('__schema__.messages_seq'),
  message_id         UUID         NOT NULL,
  created_utc        TIMESTAMP    NOT NULL,
  type               VARCHAR(128) NOT NULL,
  json_data          JSONB        NOT NULL,
  json_metadata      JSONB,
  tx_id              BIGINT       NOT NULL DEFAULT 0,
  CONSTRAINT pk_messages PRIMARY KEY (position),
  CONSTRAINT fk_messages_stream FOREIGN KEY (stream_id_internal) REFERENCES __schema__.streams (id_internal),
  CONSTRAINT uq_messages_stream_id_internal_and_stream_version UNIQUE (stream_id_internal, stream_version),
  CONSTRAINT uq_stream_id_internal_and_message_id UNIQUE (stream_id_internal, message_id),
  CONSTRAINT ck_stream_version_gte_zero CHECK (stream_version >= 0)
);

ALTER SEQUENCE __schema__.messages_seq
  OWNED BY __schema__.messages.position;

DO $F$
BEGIN
  CREATE TYPE __schema__.new_stream_message AS (
    message_id    UUID,
    "type"        VARCHAR(128),
    json_data     JSONB,
    json_metadata JSONB);
  EXCEPTION
  WHEN duplicate_object
    THEN null;
END $F$;