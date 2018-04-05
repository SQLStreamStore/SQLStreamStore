CREATE OR REPLACE FUNCTION public.append_to_stream(
  _stream_id           CHAR(42),
  _stream_id_original  VARCHAR(1000),
  _metadata_stream_id  CHAR(42),
  _expected_version    INT,
  _created_utc         TIMESTAMP,
  _new_stream_messages public.new_stream_message [])
  RETURNS TABLE(
    current_version  INT,
    current_position BIGINT,
    json_data        VARCHAR
  ) AS $F$
DECLARE
  _current_version    INT;
  _current_position   BIGINT;
  _stream_id_internal INT;
  _stream_metadata    VARCHAR;
  _success            INT;
BEGIN

  SELECT public.messages.json_data
  INTO _stream_metadata
  FROM public.messages
    JOIN public.streams
      ON public.streams.id_internal = public.messages.stream_id_internal
  WHERE public.streams.id = _metadata_stream_id
  ORDER BY public.messages.position DESC
  LIMIT 1;

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
          _stream_id_original
      ON CONFLICT DO NOTHING;

      GET DIAGNOSTICS _success = ROW_COUNT;

      IF _success = 0 AND cardinality(_new_stream_messages) > 0
      THEN
        PERFORM public.enforce_idempotent_append(
            _stream_id,
            0,
            false,
            _new_stream_messages
        );
        SELECT
          version,
          position,
          id_internal
        INTO _current_version, _current_position, _stream_id_internal
        FROM public.streams
        WHERE public.streams.id = _stream_id;

        RETURN QUERY
        SELECT
          _current_version,
          _current_position,
          _stream_metadata;
        RETURN;
      END IF;
  END IF;

  SELECT
    (
      CASE _expected_version
      WHEN -2
        THEN coalesce(
            public.read_stream_version_of_message_id(
                public.streams.id_internal,
                _new_stream_messages [1].message_id) - 1,
            public.streams.version
        )
      WHEN -1
        THEN -1
      ELSE
        _expected_version
      END
    ),
    public.streams.position,
    public.streams.id_internal
  INTO _current_version, _current_position, _stream_id_internal
  FROM public.streams
  WHERE public.streams.id = _stream_id;

  IF (_expected_version >= 0 AND _stream_id_internal IS NULL)
  THEN
    RAISE EXCEPTION 'WrongExpectedVersion';
  END IF;

  IF cardinality(_new_stream_messages) > 0
  THEN
    INSERT INTO public.messages (
      message_id,
      stream_id_internal,
      stream_version,
      created_utc,
      type,
      json_data,
      json_metadata
    )
      SELECT
        m.message_id,
        _stream_id_internal,
        _current_version + (row_number()
        over ()) :: int,
        _created_utc,
        m.type,
        m.json_data,
        m.json_metadata
      FROM unnest(_new_stream_messages) m
    ON CONFLICT DO NOTHING;
    GET DIAGNOSTICS _success = ROW_COUNT;

    IF (_success <> cardinality(_new_stream_messages))
    THEN
      IF (_expected_version = -2) /* ExpectedVersion.Any */
      THEN
        PERFORM public.enforce_idempotent_append(
            _stream_id,
            _current_version + 1 - _success,
            false,
            _new_stream_messages
        );

        SELECT
          public.messages.position,
          public.messages.stream_version
        INTO _current_position, _current_version
        FROM public.messages
        WHERE public.messages.stream_id_internal = _stream_id_internal
        ORDER BY public.messages.position DESC
        LIMIT 1;

        UPDATE public.streams
        SET "version" = _current_version, "position" = _current_position
        WHERE id_internal = _stream_id_internal;

        RETURN QUERY
        SELECT
          _current_version,
          _current_position,
          _stream_metadata;
        RETURN;
      ELSEIF _expected_version = -1 /* ExpectedVersion.NoStream */
        THEN
          RAISE EXCEPTION 'WhyAreYouHere'; /* there is no way to get here? */
      ELSE
        PERFORM public.enforce_idempotent_append(
            _stream_id,
            _expected_version + 1 - _success,
            true,
            _new_stream_messages
        );
        SELECT
          version,
          position,
          id_internal
        INTO _current_version, _current_position, _stream_id_internal
        FROM public.streams
        WHERE public.streams.id = _stream_id;

        RETURN QUERY
        SELECT
          _current_version,
          _current_position,
          _stream_metadata;
        RETURN;
      END IF;
    END IF;

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

    RETURN QUERY
    SELECT
      _current_version,
      _current_position,
      _stream_metadata;

  ELSE
    RETURN QUERY
    SELECT
      -1,
      -1 :: BIGINT,
      NULL :: VARCHAR;

  END IF;

END;

$F$
LANGUAGE 'plpgsql';