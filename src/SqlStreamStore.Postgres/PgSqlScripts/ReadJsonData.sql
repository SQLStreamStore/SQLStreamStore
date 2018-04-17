CREATE OR REPLACE FUNCTION public.read_json_data(
  _stream_id      CHAR(42),
  _stream_version INT
)
  RETURNS VARCHAR
AS $F$
BEGIN
  RETURN (
    SELECT public.messages.json_data
    FROM public.messages
      JOIN public.streams
        ON public.messages.stream_id_internal = public.streams.id_internal
    WHERE public.messages.stream_version = _stream_version
          AND public.streams.id = _stream_id
    LIMIT 1
  );
END;
$F$
LANGUAGE 'plpgsql';