CREATE OR REPLACE FUNCTION public.read(
  _stream_id CHAR(42),
  _count     INT,
  _version   INT,
  _forwards  BOOLEAN,
  _prefetch  BOOLEAN
)
  RETURNS SETOF REFCURSOR
AS $F$
DECLARE
  _stream_id_internal INT;
  _stream_info         REFCURSOR;
  _messages            REFCURSOR;
BEGIN
  SELECT public.streams.id_internal
  INTO _stream_id_internal
  FROM public.streams
  WHERE public.streams.id = _stream_id;

  OPEN _stream_info FOR
  SELECT
    public.streams.version  as stream_version,
    public.streams.position as position
  FROM public.streams
  WHERE public.streams.id_internal = _stream_id_internal;

  RETURN NEXT _stream_info;

  OPEN _messages FOR
  SELECT
    public.streams.id_original AS stream_id,
    public.messages.message_id,
    public.messages.stream_version,
    public.messages.position,
    public.messages.created_utc,
    public.messages.type,
    public.messages.json_metadata,
    (
      CASE _prefetch
      WHEN TRUE
        THEN
          public.messages.json_data
      ELSE
        NULL
      END
    )
  FROM public.messages
    INNER JOIN public.streams
      ON public.messages.stream_id_internal = public.streams.id_internal
  WHERE
    (
      CASE WHEN _forwards
        THEN
          public.messages.stream_version >= _version AND id_internal = _stream_id_internal
      ELSE
        public.messages.stream_version <= _version AND id_internal = _stream_id_internal
      END
    )
  ORDER BY
    (
      CASE WHEN _forwards
        THEN
          public.messages.stream_version
      ELSE
        -public.messages.stream_version
      END
    )
  LIMIT _count;

  RETURN NEXT _messages;
END;
$F$
LANGUAGE 'plpgsql';