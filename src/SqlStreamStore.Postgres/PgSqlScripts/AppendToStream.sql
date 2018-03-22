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

  IF expected_version = -2 /* ExpectedVersion.Any */
  THEN

    INSERT INTO public.streams (id, id_original)
      SELECT
        stream_id,
        stream_id_original
    ON CONFLICT DO NOTHING;

  ELSIF expected_version = -1 /* ExpectedVersion.NoStream */
    THEN

      INSERT INTO public.streams (id, id_original)
        SELECT
          stream_id,
          stream_id_original;

  END IF;

  SELECT
    version,
    position,
    id_internal
  INTO current_version, current_position, _stream_id_internal
  FROM public.streams
  WHERE public.streams.id = stream_id;

  IF expected_version >= 0 AND (_stream_id_internal IS NULL OR current_version != expected_version)
  THEN
    RAISE EXCEPTION 'WrongExpectedVersion';
  END IF;

  IF cardinality(new_stream_messages) > 0
  THEN

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

  ELSE
    RETURN QUERY
    SELECT
      -1,
      -1,
      CAST(NULL AS VARCHAR);

  END IF;

END;

$F$
LANGUAGE 'plpgsql';