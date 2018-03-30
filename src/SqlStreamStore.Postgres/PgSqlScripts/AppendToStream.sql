CREATE OR REPLACE FUNCTION public.append_to_stream(
  _stream_id           CHAR(42),
  _stream_id_original  VARCHAR(1000),
  _expected_version    INT,
  _created_utc         TIMESTAMP,
  _new_stream_messages public.new_stream_message [])
  RETURNS TABLE(
    current_version  INT,
    current_position BIGINT,
    json_data        VARCHAR
  ) AS $F$
DECLARE
  _current_version             INT;
  _current_position            BIGINT;
  _stream_id_internal          INT;
  _metadata_stream_id_internal INT;
  _metadata_stream_id          VARCHAR(44);
  _current_message             public.new_stream_message;

BEGIN
  SELECT '$$' || _stream_id
  INTO _metadata_stream_id;

  IF _expected_version = -2 /* ExpectedVersion.Any */
  THEN

    INSERT INTO public.streams (id, id_original)
      SELECT
        _stream_id,
        _stream_id_original
    ON CONFLICT DO NOTHING;

  ELSIF _expected_version = -1 /* ExpectedVersion.NoStream */
    THEN

      INSERT INTO public.streams (id, id_original)
        SELECT
          _stream_id,
          _stream_id_original;

  END IF;

  SELECT
    version,
    position,
    id_internal
  INTO _current_version, _current_position, _stream_id_internal
  FROM public.streams
  WHERE public.streams.id = _stream_id;

  IF _expected_version >= 0 AND (_stream_id_internal IS NULL OR _current_version != _expected_version)
  THEN
    RAISE EXCEPTION 'WrongExpectedVersion';
  END IF;

  IF cardinality(_new_stream_messages) > 0
  THEN

    FOREACH _current_message IN ARRAY _new_stream_messages
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
        _current_message.message_id,
        _stream_id_internal,
        _current_version + 1,
        _created_utc,
        _current_message.type,
        _current_message.json_data,
        _current_message.json_metadata);
      SELECT _current_version + 1
      INTO _current_version;
    END LOOP;

    SELECT
      COALESCE(public.messages.position, -1),
      COALESCE(public.messages.stream_version, -1)
    INTO _current_position, _current_version
    FROM public.messages
    WHERE public.messages.stream_id_internal = _stream_id_internal
    ORDER BY public.messages.position DESC
    LIMIT 1;

    UPDATE public.streams
    SET "version" = _current_version, "position" = _current_position
    WHERE id_internal = _stream_id_internal;
    SELECT id_internal
    INTO _metadata_stream_id_internal
    FROM public.streams
    WHERE id = _metadata_stream_id;

    RETURN QUERY
    SELECT
      _current_version,
      _current_position,
      public.messages.json_data
    FROM public.messages
    WHERE public.messages.stream_id_internal = _metadata_stream_id_internal
          OR _metadata_stream_id_internal IS NULL
    ORDER BY position DESC
    LIMIT 1;

  ELSE
    RETURN QUERY
    SELECT
      -1,
      -1::BIGINT,
      NULL ::VARCHAR;

  END IF;

END;

$F$
LANGUAGE 'plpgsql';