BEGIN TRANSACTION AppendStream;
    DECLARE @streamIdInternal AS INT;
    DECLARE @latestStreamVersion AS INT;

     SELECT @streamIdInternal = dbo.Streams.IdInternal,
            @latestStreamVersion = dbo.Streams.[Version]
       FROM dbo.Streams
      WHERE dbo.Streams.Id = @streamId;

         IF @streamIdInternal IS NULL
        BEGIN
            ROLLBACK TRANSACTION AppendStream;
            RAISERROR('WrongExpectedVersion', 16, 1);
            RETURN;
        END

        IF @latestStreamVersion != @expectedStreamVersion
        BEGIN
            ROLLBACK TRANSACTION AppendStream;
            RAISERROR('WrongExpectedVersion', 16, 2);
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
       FROM @newMessages
   ORDER BY StreamVersion;

  SELECT TOP(1)
            @latestStreamVersion = dbo.Events.StreamVersion
       FROM dbo.Events
      WHERE dbo.Events.StreamIDInternal = @streamIdInternal
   ORDER BY dbo.Events.Ordinal DESC

     UPDATE dbo.Streams
        SET dbo.Streams.[Version] = @latestStreamVersion
      WHERE dbo.Streams.IdInternal = @streamIdInternal

COMMIT TRANSACTION AppendStream;

/* Select Metadata */
    DECLARE @metadataStreamId as NVARCHAR(42)
    DECLARE @metadataStreamIdInternal as INT
        SET @metadataStreamId = '$$' + @streamId

     SELECT @metadataStreamIdInternal = dbo.Streams.IdInternal
       FROM dbo.Streams
      WHERE dbo.Streams.Id = @metadataStreamId;

     SELECT TOP(1)
            dbo.Events.JsonData
       FROM dbo.Events
      WHERE dbo.Events.StreamIdInternal = @metadataStreamIdInternal
   ORDER BY dbo.Events.Ordinal DESC;