BEGIN TRANSACTION CreateStream;
    DECLARE @streamIdInternal AS INT;
    DECLARE @latestStreamVersion AS INT;

    BEGIN
        INSERT INTO dbo.Streams (Id, IdOriginal) VALUES (@streamId, @streamIdOriginal);
        SELECT @streamIdInternal = SCOPE_IDENTITY();

        INSERT INTO dbo.Events (StreamIdInternal, StreamVersion, Id, Created, [Type], JsonData, JsonMetadata)
             SELECT @streamIdInternal,
                    StreamVersion,
                    Id,
                    Created,
                    [Type],
                    JsonData,
                    JsonMetadata
               FROM @newEvents
           ORDER BY StreamVersion;

             SELECT TOP(1)
                    @latestStreamVersion = dbo.Events.StreamVersion
              FROM dbo.Events
             WHERE dbo.Events.StreamIDInternal = @streamIdInternal
          ORDER BY dbo.Events.Ordinal DESC

            UPDATE dbo.Streams
               SET dbo.Streams.[Version] = @latestStreamVersion
             WHERE dbo.Streams.IdInternal = @streamIdInternal
    END;
    SELECT @streamIdInternal;
COMMIT TRANSACTION CreateStream;
