INSERT INTO $schema$.streams(id, id_original)
VALUES (:stream_id, :stream_id_original)
RETURNING id_internal;