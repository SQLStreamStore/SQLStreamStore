BEGIN TRANSACTION DeleteStream

        DECLARE @streamIdInternal AS INT

         SELECT @streamIdInternal = dbo.Streams.IdInternal
           FROM dbo.Streams
          WHERE dbo.Streams.Id = @streamId;

    DELETE FROM dbo.Messages
          WHERE dbo.Messages.StreamIdInternal = @streamIdInternal;

    DELETE FROM dbo.Streams
          WHERE dbo.Streams.Id = @streamId;
         SELECT @@ROWCOUNT 

COMMIT TRANSACTION DeleteStream

DELETE FROM messages
WHERE messages.stream_id_internal = (
      SELECT TOP 1 streams.id_internal 
      FROM streams
      WHERE streams.id = @streamId);

DELETE FROM streams
      WHERE streams.id = @streamId;
SELECT @@ROWCOUNT