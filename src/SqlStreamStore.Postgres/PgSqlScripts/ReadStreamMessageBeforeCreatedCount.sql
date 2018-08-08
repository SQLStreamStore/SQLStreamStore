CREATE OR REPLACE FUNCTION __schema__.read_stream_message_before_created_count(
  _stream_id   CHAR(42),
  _created_utc TIMESTAMP
)
  RETURNS INT
AS $F$
DECLARE
  _stream_id_internal INT;
BEGIN
  SELECT __schema__.streams.id_internal
      INTO _stream_id_internal
  FROM __schema__.streams
  WHERE __schema__.streams.id = _stream_id;
  RETURN (SELECT count(*)
          FROM __schema__.messages
          WHERE __schema__.messages.stream_id_internal = _stream_id_internal
            AND __schema__.messages.created_utc < _created_utc);
END;
$F$
LANGUAGE 'plpgsql';