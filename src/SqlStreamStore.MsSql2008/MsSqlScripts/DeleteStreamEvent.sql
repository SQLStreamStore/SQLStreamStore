BEGIN TRANSACTION DeleteStream
        DECLARE @streamIdInternal AS INT

         SELECT @streamIdInternal = dbo.Streams.IdInternal
           FROM dbo.Streams
          WHERE dbo.Streams.Id = @streamId;

    DELETE FROM dbo.Events
          WHERE dbo.Events.StreamIdInternal = @streamIdInternal AND dbo.Events.Id = @eventId
         SELECT @@ROWCOUNT AS DELETED;

COMMIT TRANSACTION DeleteStream