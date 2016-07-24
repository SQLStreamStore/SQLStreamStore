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