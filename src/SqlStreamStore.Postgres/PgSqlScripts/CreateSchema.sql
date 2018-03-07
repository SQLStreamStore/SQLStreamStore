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

CREATE OR REPLACE FUNCTION public.read(
  _stream_id CHAR(42),
  _count     INT,
  _position  BIGINT,
  _version   INT,
  _forwards  BOOLEAN,
  _prefetch  BOOLEAN
)
  RETURNS SETOF REFCURSOR
AS $F$
DECLARE 
  _stream_id_internal INT;
  stream_info REFCURSOR;
  messages REFCURSOR;
BEGIN
  SELECT public.streams.id_internal
  INTO _stream_id_internal
  FROM public.streams
  WHERE public.streams.id = _stream_id;
  
  OPEN stream_info FOR 
  SELECT
    public.streams.version     as stream_version,
    public.streams.position    as position
  FROM public.streams
  WHERE public.streams.id_internal = _stream_id_internal;
  
  RETURN NEXT stream_info;
  
  OPEN messages FOR
  SELECT
    public.streams.id_original AS stream_id,
    public.messages.message_id,
    public.messages.stream_version,
    public.messages.position,
    public.messages.created_utc,
    public.messages.type,
    public.messages.json_metadata,
    (
      CASE _prefetch
      WHEN TRUE
        THEN
          public.messages.json_data
      ELSE
        NULL
      END
    )
  FROM public.messages
    INNER JOIN public.streams
      ON public.messages.stream_id_internal = public.streams.id_internal
  WHERE
    (
      CASE WHEN _forwards
        THEN
          CASE WHEN _stream_id IS NULL
            THEN
              public.messages.position >= _position
          ELSE
            public.messages.stream_version >= _version AND id_internal = _stream_id_internal
          END
      ELSE
        CASE WHEN _stream_id IS NULL
          THEN
            public.messages.position <= _position
        ELSE
          public.messages.stream_version <= _version AND id_internal = _stream_id_internal
        END
      END
    )
  ORDER BY 
    (
      CASE WHEN _forwards
        THEN
          public.messages.stream_version
        ELSE
          -public.messages.stream_version
        END
    )
  LIMIT _count;
  
  RETURN NEXT messages;
END;
$F$
LANGUAGE 'plpgsql';

CREATE OR REPLACE FUNCTION public.append_to_stream(
  stream_id           CHAR(42),
  stream_id_original  VARCHAR(1000),
  expected_version    INT,
  created_utc         TIMESTAMP,
  new_stream_messages public.new_stream_message [])
  RETURNS TABLE(
    current_version  INT,
    current_position BIGINT,
    json_data        VARCHAR
  ) AS $F$
DECLARE
  current_version             INT;
  current_position            BIGINT;
  _stream_id_internal         INT;
  metadata_stream_id_internal INT;
  metadata_stream_id          VARCHAR(44);
  current_message             public.new_stream_message;

BEGIN
  SELECT '$$' || stream_id
  INTO metadata_stream_id;

  IF expected_version = -2
  THEN /* ExpectedVersion.Any */

    INSERT INTO public.streams (id, id_original)
      SELECT
        stream_id,
        stream_id_original
    ON CONFLICT DO NOTHING;

    SELECT
      version,
      position,
      id_internal
    INTO current_version, current_position, _stream_id_internal
    FROM public.streams
    WHERE public.streams.id = stream_id;

    FOREACH current_message IN ARRAY new_stream_messages
    LOOP
      INSERT INTO public.messages (
        message_id,
        stream_id_internal,
        stream_version,
        created_utc,
        type,
        json_data,
        json_metadata
      ) VALUES (
        current_message.message_id,
        _stream_id_internal,
        current_version + 1,
        created_utc,
        current_message.type,
        current_message.json_data,
        current_message.json_metadata);
      SELECT current_version + 1
      INTO current_version;
    END LOOP;

    SELECT
      COALESCE(public.messages.position, -1),
      COALESCE(public.messages.stream_version, -1)
    INTO current_position, current_version
    FROM public.messages
    WHERE public.messages.stream_id_internal = _stream_id_internal
    ORDER BY public.messages.position DESC
    LIMIT 1;

    UPDATE public.streams
    SET "version" = current_version, "position" = current_position
    WHERE id_internal = _stream_id_internal;

  END IF;

  SELECT id_internal
  INTO metadata_stream_id_internal
  FROM public.streams
  WHERE id = metadata_stream_id;

  RETURN QUERY
  SELECT
    current_version,
    current_position,
    public.messages.json_data
  FROM public.messages
  WHERE public.messages.stream_id_internal = metadata_stream_id_internal
        OR metadata_stream_id_internal IS NULL
  ORDER BY position DESC
  LIMIT 1;

END;

$F$
LANGUAGE 'plpgsql';