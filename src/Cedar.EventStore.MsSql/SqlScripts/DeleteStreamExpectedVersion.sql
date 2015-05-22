SET TRANSACTION ISOLATION LEVEL SERIALIZABLE;
BEGIN TRANSACTION DeleteStream
        DECLARE @streamIdInternal AS INT;
        DECLARE @latestStreamRevision  AS INT;

         SELECT @streamIdInternal = Streams.IdInternal
           FROM Streams
          WHERE Streams.Id = @streamId;

          IF @streamIdInternal IS NULL
          BEGIN
             ROLLBACK TRANSACTION DeleteStream;
             RAISERROR('WrongExpectedVersion', 1,1);
             RETURN;
          END

          SELECT TOP(1)
                @latestStreamRevision = Events.StreamRevision
           FROM Events
          WHERE Events.StreamIDInternal = @streamIdInternal
       ORDER BY Events.Ordinal DESC;

         IF @latestStreamRevision != @expectedStreamRevision
         BEGIN
            ROLLBACK TRANSACTION DeleteStream;
            RAISERROR('WrongExpectedVersion', 1,2);
            RETURN;
         END

         UPDATE Streams
            SET IsDeleted = '1'
          WHERE Streams.Id = @streamId ;

         DELETE FROM Events
          WHERE Events.StreamIdInternal = @streamIdInternal;

COMMIT TRANSACTION DeleteStream