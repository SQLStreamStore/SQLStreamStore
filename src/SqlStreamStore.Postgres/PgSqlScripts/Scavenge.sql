CREATE OR REPLACE FUNCTION public.scavenge(
  _stream_id CHAR(42),
  _max_count INT
)
  RETURNS TABLE(
    message_id UUID
  ) AS $F$
DECLARE
  _stream_id_internal INT;
  _message_ids        UUID [];
BEGIN
  SELECT public.streams.id_internal
  INTO _stream_id_internal
  FROM public.streams
  WHERE public.streams.id = _stream_id;

  _message_ids = ARRAY(
      SELECT public.messages.message_id
      FROM public.messages
      WHERE public.messages.stream_id_internal = _stream_id_internal
      ORDER BY public.messages.stream_version
      LIMIT coalesce(_max_count, 0)
  );

  RETURN QUERY
  SELECT *
  FROM unnest(_message_ids);

END;
$F$
LANGUAGE 'plpgsql';