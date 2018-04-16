CREATE OR REPLACE FUNCTION public.delete_stream(
  _stream_id                  CHAR(42),
  _expected_version           INT,
  _created_utc                TIMESTAMP,
  _deleted_stream_id          CHAR(42),
  _deleted_stream_id_original VARCHAR(1000),
  _deleted_stream_message     public.new_stream_message)
  RETURNS VOID
AS $F$
DECLARE
  _stream_id_internal    INT;
  _latest_stream_version INT;
  _affected              INT;
BEGIN
  SELECT public.streams.id_internal
  INTO _stream_id_internal
  FROM public.streams
  WHERE public.streams.id = _stream_id;

  IF _expected_version = -1 /* ExpectedVersion.NoStream */
  THEN
    RAISE EXCEPTION 'WrongExpectedVersion';
  ELSIF _expected_version >= 0 /* ExpectedVersion */
    THEN
      IF _stream_id_internal IS NULL
      THEN
        RAISE EXCEPTION 'WrongExpectedVersion';
      END IF;

      SELECT public.messages.stream_version
      INTO _latest_stream_version
      FROM public.messages
      WHERE public.messages.stream_id_internal = _stream_id_internal
      ORDER BY public.messages.position DESC
      LIMIT 1;

      IF _latest_stream_version != _expected_version
      THEN
        RAISE EXCEPTION 'WrongExpectedVersion';
      END IF;
  END IF;

  DELETE
  FROM public.messages
  WHERE public.messages.stream_id_internal = _stream_id_internal;

  DELETE
  FROM public.streams
  WHERE public.streams.id = _stream_id;

  GET DIAGNOSTICS _affected = ROW_COUNT;

  IF _affected > 0
  THEN
    PERFORM public.append_to_stream(
        _deleted_stream_id,
        _deleted_stream_id_original,
        -2,
        _created_utc,
        ARRAY [_deleted_stream_message]
    );
  END IF;
END;

$F$
LANGUAGE 'plpgsql';