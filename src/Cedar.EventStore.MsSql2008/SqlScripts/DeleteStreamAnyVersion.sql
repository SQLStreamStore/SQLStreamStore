SET TRANSACTION ISOLATION LEVEL SERIALIZABLE;
BEGIN TRANSACTION DeleteStream
        DECLARE @streamIdInternal AS INT

         SELECT @streamIdInternal = dbo.Streams.IdInternal
           FROM dbo.Streams
          WHERE dbo.Streams.Id = @streamId;

    DELETE FROM dbo.Events
          WHERE dbo.Events.StreamIdInternal = @streamIdInternal;

         UPDATE dbo.Streams
            SET IsDeleted = '1'
          WHERE dbo.Streams.Id = @streamId;
COMMIT TRANSACTION DeleteStream