CREATE OR REPLACE FUNCTION public.read_all(
  _count    INT,
  _position BIGINT,
  _forwards BOOLEAN,
  _prefetch BOOLEAN
)
  RETURNS TABLE(
    stream_id      VARCHAR(1000),
    message_id     UUID,
    stream_version INT,
    "position"     BIGINT,
    create_utc     TIMESTAMP,
    "type"         VARCHAR(128),
    json_metadata  VARCHAR,
    json_data      VARCHAR
  )
AS $F$
BEGIN

  RETURN QUERY
  SELECT
    public.streams.id_original,
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
          public.messages.position >= _position
      ELSE
        public.messages.position <= _position
      END
    )
  ORDER BY
    (
      CASE WHEN _forwards
        THEN
          public.messages.position
      ELSE
        -public.messages.position
      END
    )
  LIMIT _count;
END;
$F$
LANGUAGE 'plpgsql';