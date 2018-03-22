CREATE SEQUENCE IF NOT EXISTS public.streams_seq
  START 1;

CREATE TABLE IF NOT EXISTS public.streams (
  id          CHAR(44)      NOT NULL,
  id_original VARCHAR(1000) NOT NULL,
  id_internal INT           NOT NULL DEFAULT nextval('public.streams_seq') PRIMARY KEY,
  version     INT           NOT NULL DEFAULT (-1),
  position    BIGINT        NOT NULL DEFAULT (-1)
);

ALTER SEQUENCE public.streams_seq
OWNED BY public.streams.id_internal;

CREATE SEQUENCE IF NOT EXISTS public.messages_seq
  START 0
  MINVALUE 0;

CREATE TABLE IF NOT EXISTS public.messages (
  stream_id_internal INT          NOT NULL,
  stream_version     INT          NOT NULL,
  "position"         BIGINT       NOT NULL default nextval('public.messages_seq') PRIMARY KEY,
  message_id         UUID         NOT NULL,
  created_utc        TIMESTAMP    NOT NULL,
  type               VARCHAR(128) NOT NULL,
  json_data          VARCHAR      NOT NULL,
  json_metadata      VARCHAR
);

ALTER SEQUENCE public.messages_seq
OWNED BY public.messages.position;

DROP TYPE IF EXISTS public.new_stream_message;
CREATE TYPE public.new_stream_message AS (
  message_id    UUID,
  "type"        VARCHAR(128),
  json_data     VARCHAR,
  json_metadata VARCHAR
);