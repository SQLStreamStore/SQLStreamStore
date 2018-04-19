CREATE OR REPLACE FUNCTION __schema__.read_all(
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
    __schema__.streams.id_original,
    __schema__.messages.message_id,
    __schema__.messages.stream_version,
    __schema__.messages.position,
    __schema__.messages.created_utc,
    __schema__.messages.type,
    __schema__.messages.json_metadata,
    (
      CASE _prefetch
      WHEN TRUE
        THEN
          __schema__.messages.json_data
      ELSE
        NULL
      END
    )
  FROM __schema__.messages
    INNER JOIN __schema__.streams
      ON __schema__.messages.stream_id_internal = __schema__.streams.id_internal
  WHERE
    (
      CASE WHEN _forwards
        THEN
          __schema__.messages.position >= _position
      ELSE
        __schema__.messages.position <= _position
      END
    )
  ORDER BY
    (
      CASE WHEN _forwards
        THEN
          __schema__.messages.position
      ELSE
        -__schema__.messages.position
      END
    )
  LIMIT _count;
END;
$F$
LANGUAGE 'plpgsql';