CREATE SEQUENCE IF NOT EXISTS public.streams_seq
  START 1;

CREATE TABLE IF NOT EXISTS public.streams (
  id          CHAR(42)      NOT NULL,
  id_original VARCHAR(1000) NOT NULL,
  id_internal INT           NOT NULL DEFAULT nextval('public.streams_seq'),
  version     INT           NOT NULL DEFAULT (-1),
  position    BIGINT        NOT NULL DEFAULT (-1),
  CONSTRAINT pk_streams PRIMARY KEY (id_internal)
);

ALTER SEQUENCE public.streams_seq
OWNED BY public.streams.id_internal;

CREATE UNIQUE INDEX IF NOT EXISTS streams_by_id
  ON public.streams (id);

CREATE SEQUENCE IF NOT EXISTS public.messages_seq
  START 0
  MINVALUE 0;

CREATE TABLE IF NOT EXISTS public.messages (
  stream_id_internal INT          NOT NULL,
  stream_version     INT          NOT NULL,
  "position"         BIGINT       NOT NULL DEFAULT nextval('public.messages_seq'),
  message_id         UUID         NOT NULL,
  created_utc        TIMESTAMP    NOT NULL,
  type               VARCHAR(128) NOT NULL,
  json_data          VARCHAR      NOT NULL,
  json_metadata      VARCHAR,
  CONSTRAINT pk_messages PRIMARY KEY (position),
  CONSTRAINT fk_messages_stream FOREIGN KEY (stream_id_internal) REFERENCES public.streams (id_internal)
);

ALTER SEQUENCE public.messages_seq
OWNED BY public.messages.position;

CREATE UNIQUE INDEX IF NOT EXISTS messages_by_position
  ON public.messages (position);

CREATE UNIQUE INDEX IF NOT EXISTS messages_by_stream_id_internal_and_message_id
  ON public.messages (stream_id_internal, message_id);

CREATE UNIQUE INDEX IF NOT EXISTS messages_by_stream_id_internal_and_stream_version
  ON public.messages (stream_id_internal, stream_version);

CREATE INDEX IF NOT EXISTS messages_by_stream_id_internal_and_created_utc
  ON public.messages (stream_id_internal, created_utc);

CREATE TYPE public.new_stream_message AS (
  message_id    UUID,
  "type"        VARCHAR(128),
  json_data     VARCHAR,
  json_metadata VARCHAR
);