CREATE OR REPLACE FUNCTION __schema__.read_stream_head_version(
  _stream_id      CHAR(42)
)
  RETURNS INT
AS $F$
BEGIN
  RETURN (SELECT (__schema__.streams.version) FROM __schema__.streams WHERE __schema__.streams.id = _stream_id);
END;
$F$
LANGUAGE 'plpgsql';