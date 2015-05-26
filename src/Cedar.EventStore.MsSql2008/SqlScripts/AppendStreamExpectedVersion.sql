SET TRANSACTION ISOLATION LEVEL SERIALIZABLE;
BEGIN TRANSACTION AppendStream;
    DECLARE @streamIdInternal AS INT;
    DECLARE @latestStreamVersion  AS INT;

     SELECT @streamIdInternal = Streams.IdInternal
       FROM Streams
      WHERE Streams.Id = @streamId;

         IF @streamIdInternal IS NULL
        BEGIN
            ROLLBACK TRANSACTION AppendStream;
            RAISERROR('WrongExpectedVersion', 1,1);
            RETURN;
        END

        SELECT TOP(1)
             @latestStreamVersion = Events.StreamVersion
        FROM Events
       WHERE Events.StreamIDInternal = @streamIdInternal
    ORDER BY Events.Ordinal DESC;

        IF @latestStreamVersion != @expectedStreamVersion
        BEGIN
            ROLLBACK TRANSACTION AppendStream;
            RAISERROR('WrongExpectedVersion', 1,2);
            RETURN;
        END

INSERT INTO dbo.Events (StreamIdInternal, StreamVersion, Id, Created, [Type], JsonData, JsonMetadata)
     SELECT @streamIdInternal,
            StreamVersion + @latestStreamVersion + 1,
            Id,
            Created,
            [Type],
            JsonData,
            JsonMetadata
       FROM @newEvents;
 
COMMIT TRANSACTION AppendStream;
