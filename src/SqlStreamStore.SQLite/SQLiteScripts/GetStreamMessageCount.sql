SELECT COUNT(*)
FROM messages
where messages.stream_id_internal = (
    SELECT TOP 1 id_internal 
    FROM streams 
    WHERE id = @streamId)