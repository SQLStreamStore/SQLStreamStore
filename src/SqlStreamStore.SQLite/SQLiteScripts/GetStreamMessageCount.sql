SELECT COUNT(*)
FROM messages
where messages.stream_id_internal = (
    SELECT id_internal 
    FROM streams 
    WHERE id = @streamId
    LIMIT 1)