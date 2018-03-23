CREATE OR REPLACE FUNCTION public.delete_stream(
  _stream_id        CHAR(42),
  _expected_version INT)
  RETURNS INT
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
  
  RETURN _affected;
END;

$F$
LANGUAGE 'plpgsql';