CREATE OR REPLACE FUNCTION __schema__.read_stream_version_of_message_id(
  _stream_id_internal INT,
  _message_id         UUID
)
  RETURNS INT
AS $F$
BEGIN
  RETURN (
    SELECT __schema__.messages.stream_version
    FROM __schema__.messages
    WHERE __schema__.messages.message_id = _message_id
    AND __schema__.messages.stream_id_internal = _stream_id_internal
  );
END;
$F$
LANGUAGE 'plpgsql';