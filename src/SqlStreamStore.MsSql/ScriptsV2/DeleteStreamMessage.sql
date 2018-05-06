BEGIN TRANSACTION DeleteStream
        DECLARE @streamIdInternal AS INT

         SELECT @streamIdInternal = dbo.Streams.IdInternal
           FROM dbo.Streams
          WHERE dbo.Streams.Id = @streamId;

    DELETE FROM dbo.Messages
          WHERE dbo.Messages.StreamIdInternal = @streamIdInternal AND dbo.Messages.Id = @eventId
         SELECT @@ROWCOUNT AS DELETED;

COMMIT TRANSACTION DeleteStream