DROP PROCEDURE IF EXISTS read_stream_head_version;

CREATE PROCEDURE read_stream_head_version(_stream_id CHAR(42))

BEGIN
  SELECT streams.version
  FROM streams
  WHERE streams.id = _stream_id;
END;