DROP PROCEDURE IF EXISTS `read`;
CREATE PROCEDURE `read`(_stream_id CHAR(42),
                      _count INT,
                      _version INT,
                      _forwards BOOLEAN,
                      _prefetch BOOLEAN)
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
         (CASE _prefetch
            WHEN TRUE THEN messages.json_data
            ELSE NULL END)
  FROM messages
         INNER JOIN streams ON messages.stream_id_internal = streams.id_internal
  WHERE (CASE
           WHEN _forwards THEN messages.stream_version >= _version AND id_internal = _stream_id_internal
           ELSE messages.stream_version <= _version AND id_internal = _stream_id_internal END)
  ORDER BY (CASE
              WHEN _forwards THEN messages.stream_version
              ELSE -messages.stream_version END)
  LIMIT _count;

END;