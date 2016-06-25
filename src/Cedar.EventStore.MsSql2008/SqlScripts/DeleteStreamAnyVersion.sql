BEGIN TRANSACTION DeleteStream
        DECLARE @streamIdInternal AS INT

         SELECT @streamIdInternal = dbo.Streams.IdInternal
           FROM dbo.Streams
          WHERE dbo.Streams.Id = @streamId;

    DELETE FROM dbo.Events
          WHERE dbo.Events.StreamIdInternal = @streamIdInternal;

    DELETE FROM dbo.Streams
          WHERE dbo.Streams.Id = @streamId;
COMMIT TRANSACTION DeleteStream