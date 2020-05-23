CREATE OR REPLACE FUNCTION __schema__.read_stream_head_position(
  _stream_id      CHAR(42),
)
  RETURNS BIGINT
AS $F$
BEGIN
  RETURN (SELECT (__schema__.streams.position) FROM __schema__.streams WHERE __schema__.streams.id = _stream_id);
END;
$F$
LANGUAGE 'plpgsql';