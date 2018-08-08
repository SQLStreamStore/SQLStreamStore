CREATE OR REPLACE FUNCTION __schema__.scavenge(
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
  SELECT __schema__.streams.id_internal
      INTO _stream_id_internal
  FROM __schema__.streams
  WHERE __schema__.streams.id = _stream_id;

  _message_ids = ARRAY(
      SELECT __schema__.messages.message_id
      FROM __schema__.messages
      WHERE __schema__.messages.stream_id_internal = _stream_id_internal
      ORDER BY __schema__.messages.stream_version
      LIMIT coalesce(_max_count, 0)
  );

  RETURN QUERY
  SELECT * FROM unnest(_message_ids);

END;
$F$
LANGUAGE 'plpgsql';