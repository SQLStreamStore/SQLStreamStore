CREATE OR REPLACE FUNCTION public.read_stream_version_of_message_id(
  _stream_id_internal INT,
  _message_id         UUID
)
  RETURNS INT
AS $F$
BEGIN
  RETURN (
    SELECT public.messages.stream_version
    FROM public.messages
    WHERE public.messages.message_id = _message_id
    AND public.messages.stream_id_internal = _stream_id_internal
  );
END;
$F$
LANGUAGE 'plpgsql';