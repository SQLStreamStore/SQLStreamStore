CREATE OR REPLACE FUNCTION public.delete_stream_message(
  _stream_id                  CHAR(42),
  _message_id                 UUID,
  _deleted_stream_id          CHAR(42),
  _deleted_stream_id_original VARCHAR(1000),
  _metadata_stream_id         CHAR(42),
  _created_utc                TIMESTAMP,
  _deleted_message            public.new_stream_message []
)
  RETURNS VOID
AS $F$
DECLARE
  _stream_id_internal INT;
  _deleted_count      NUMERIC;
BEGIN

  SELECT public.streams.id_internal
  INTO _stream_id_internal
  FROM public.streams
  WHERE public.streams.id = _stream_id;

  WITH deleted AS
  (
    DELETE FROM public.messages
    WHERE public.messages.stream_id_internal = _stream_id_internal
          AND public.messages.message_id = _message_id
                                           IS TRUE
    RETURNING *
  )
  SELECT count(*)
  FROM deleted
  INTO _deleted_count;

  IF _deleted_count > 0
  THEN
    PERFORM public.append_to_stream(
        _deleted_stream_id,
        _deleted_stream_id_original,
        _metadata_stream_id,
        -2,
        _created_utc,
        _deleted_message);
  END IF;
END;

$F$
LANGUAGE 'plpgsql';