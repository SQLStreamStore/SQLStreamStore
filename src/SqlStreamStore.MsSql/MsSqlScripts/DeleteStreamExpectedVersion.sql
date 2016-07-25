BEGIN TRANSACTION DeleteStream
        DECLARE @streamIdInternal AS INT;
        DECLARE @latestStreamVersion  AS INT;

         SELECT @streamIdInternal = dbo.Streams.IdInternal
           FROM dbo.Streams
          WHERE dbo.Streams.Id = @streamId;

          IF @streamIdInternal IS NULL
          BEGIN
             RAISERROR('WrongExpectedVersion', 16,1);
             RETURN;
          END

          SELECT TOP(1)
                @latestStreamVersion = dbo.Messages.StreamVersion
           FROM dbo.Messages
          WHERE dbo.Messages.StreamIDInternal = @streamIdInternal
       ORDER BY dbo.Messages.Ordinal DESC;

         IF @latestStreamVersion != @expectedStreamVersion
         BEGIN
            RAISERROR('WrongExpectedVersion', 16,2);
            RETURN;
         END

         DELETE FROM dbo.Messages
          WHERE dbo.Messages.StreamIdInternal = @streamIdInternal;

         DELETE FROM dbo.Streams
          WHERE dbo.Streams.Id = @streamId;

COMMIT TRANSACTION DeleteStream