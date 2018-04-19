CREATE OR REPLACE FUNCTION __schema__.read_json_data(
  _stream_id      CHAR(42),
  _stream_version INT
)
  RETURNS VARCHAR
AS $F$
BEGIN
  RETURN (
    SELECT __schema__.messages.json_data
    FROM __schema__.messages
      JOIN __schema__.streams
        ON __schema__.messages.stream_id_internal = __schema__.streams.id_internal
    WHERE __schema__.messages.stream_version = _stream_version
          AND __schema__.streams.id = _stream_id
    LIMIT 1
  );
END;
$F$
LANGUAGE 'plpgsql';