CREATE OR REPLACE FUNCTION public.read_stream_version_of_message_id(
  _stream_id  CHAR(42),
  _message_id UUID
)
  RETURNS INT
AS $F$
DECLARE
  _stream_id_internal INT;
BEGIN
  SELECT public.streams.id_internal
  INTO _stream_id_internal
  FROM public.streams
  WHERE public.streams.id = _stream_id;

  RETURN (
    SELECT public.streams.stream_version
    FROM public.messages
    WHERE public.messages.stream_id_internal = _stream_id_internal
          AND public.messages.message_id = _message_id
  );
END;
$F$
LANGUAGE 'plpgsql';