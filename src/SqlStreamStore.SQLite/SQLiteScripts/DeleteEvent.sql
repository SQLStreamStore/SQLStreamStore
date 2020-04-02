DELETE FROM messages
WHERE messages.stream_id_internal = (
      SELECT TOP 1 streams.id_internal 
      FROM streams 
      WHERE streams.id = @streamId) 
  AND messages.id = @eventId;
SELECT @@ROWCOUNT AS DELETED;