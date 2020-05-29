DROP PROCEDURE IF EXISTS read_stream_head_position;

CREATE PROCEDURE read_stream_head_position(_stream_id CHAR(42))

BEGIN
  SELECT streams.position
  FROM streams
  WHERE streams.id = _stream_id;
END;