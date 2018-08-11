CREATE OR REPLACE FUNCTION __schema__.scavenge(
  _stream_id CHAR(42)
)
  RETURNS TABLE(
    message_id UUID
  ) AS $F$
DECLARE
  _stream_id_internal INT;
  _max_count          INT;
BEGIN
  SELECT __schema__.streams.id_internal, __schema__.streams.max_count
      INTO _stream_id_internal, _max_count
  FROM __schema__.streams
  WHERE __schema__.streams.id = _stream_id;

  IF (_max_count IS NULL)
  THEN RETURN;
  END IF;

  RETURN QUERY
  SELECT __schema__.messages.message_id
  FROM __schema__.messages
  WHERE __schema__.messages.stream_id_internal = _stream_id_internal
    AND __schema__.messages.message_id NOT IN(SELECT __schema__.messages.message_id
                                              FROM __schema__.messages
                                              WHERE __schema__.messages.stream_id_internal = _stream_id_internal
                                              ORDER BY __schema__.messages.stream_version desc
                                              LIMIT _max_count)
  ORDER BY __schema__.messages.stream_version;

END;
$F$
LANGUAGE 'plpgsql';