SET TRANSACTION ISOLATION LEVEL SERIALIZABLE;
BEGIN TRANSACTION DeleteStream
        DECLARE @streamIdInternal AS INT;
        DECLARE @latestStreamVersion  AS INT;

         SELECT @streamIdInternal = dbo.Streams.IdInternal
           FROM Streams
          WHERE Streams.Id = @streamId;

          IF @streamIdInternal IS NULL
          BEGIN
             ROLLBACK TRANSACTION DeleteStream;
             RAISERROR('WrongExpectedVersion', 16,1);
             RETURN;
          END

          SELECT TOP(1)
                @latestStreamVersion = dbo.Events.StreamVersion
           FROM dbo.Events
          WHERE dbo.Events.StreamIDInternal = @streamIdInternal
       ORDER BY dbo.Events.Ordinal DESC;

         IF @latestStreamVersion != @expectedStreamVersion
         BEGIN
            ROLLBACK TRANSACTION DeleteStream;
            RAISERROR('WrongExpectedVersion', 16,2);
            RETURN;
         END

         UPDATE dbo.Streams
            SET IsDeleted = '1'
          WHERE dbo.Streams.Id = @streamId ;

         DELETE FROM dbo.Events
          WHERE dbo.Events.StreamIdInternal = @streamIdInternal;

COMMIT TRANSACTION DeleteStream