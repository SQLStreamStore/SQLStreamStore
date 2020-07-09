DROP PROCEDURE IF EXISTS `read_stream_forwards_with_data`;
CREATE PROCEDURE `read_stream_forwards_with_data`(_stream_id CHAR(42),
                      _count INT,
                      _version INT)
BEGIN
  DECLARE _stream_id_internal INT;

  SELECT streams.id_internal INTO _stream_id_internal
  FROM streams
  WHERE streams.id = _stream_id;

  SELECT streams.version  as stream_version,
         streams.position as position,
         streams.max_age  as max_age
  FROM streams
  WHERE streams.id_internal = _stream_id_internal;

  SELECT streams.id_original AS stream_id,
         messages.message_id,
         messages.stream_version,
         messages.position - 1,
         messages.created_utc,
         messages.type,
         messages.json_metadata,
         messages.json_data
  FROM messages
         STRAIGHT_JOIN streams ON messages.stream_id_internal = streams.id_internal
  WHERE messages.stream_version >= _version AND id_internal = _stream_id_internal
  ORDER BY messages.stream_version ASC
  LIMIT _count;

END;