DROP PROCEDURE IF EXISTS read_all;

CREATE PROCEDURE read_all(_count INT,
                          _position BIGINT,
                          _forwards BOOLEAN,
                          _prefetch BOOLEAN)

BEGIN
  SELECT streams.id_original,
         messages.message_id,
         messages.stream_version,
         messages.position - 1,
         messages.created_utc,
         messages.type,
         messages.json_metadata,
         (CASE _prefetch
            WHEN TRUE THEN messages.json_data
            ELSE NULL END),
         streams.max_age
  FROM messages
         INNER JOIN streams ON messages.stream_id_internal = streams.id_internal
  WHERE (CASE
           WHEN _forwards THEN messages.position >= _position + 1
           ELSE messages.position <= _position + 1 END)
  ORDER BY (CASE
              WHEN _forwards THEN messages.position
              ELSE -messages.position END)
  LIMIT _count;

END;