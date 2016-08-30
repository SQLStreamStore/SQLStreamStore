DECLARE @streamIdInternal AS INT;

BEGIN TRANSACTION CreatStreamIfNotExists;
    IF NOT EXISTS (SELECT * FROM dbo.Streams WITH (UPDLOCK, ROWLOCK, HOLDLOCK)
                     WHERE dbo.Streams.Id = @streamId)
     INSERT INTO dbo.Streams (Id, IdOriginal) VALUES (@streamId, @streamIdOriginal);

COMMIT TRANSACTION CreatStreamIfNotExists;

BEGIN TRANSACTION AppendStream;
    DECLARE @latestStreamVersion AS INT;

     SELECT @streamIdInternal = dbo.Streams.IdInternal,
            @latestStreamVersion = dbo.Streams.[Version]
       FROM dbo.Streams WITH (UPDLOCK, ROWLOCK)
      WHERE dbo.Streams.Id = @streamId;

INSERT INTO dbo.Messages (StreamIdInternal, StreamVersion, Id, Created, [Type], JsonData, JsonMetadata)
     SELECT @streamIdInternal,
            StreamVersion + @latestStreamVersion + 1,
            Id,
            Created,
            [Type],
            JsonData,
            JsonMetadata
        FROM @newMessages
    ORDER BY StreamVersion

     SELECT TOP(1)
            @latestStreamVersion = dbo.Messages.StreamVersion
       FROM dbo.Messages
      WHERE dbo.Messages.StreamIDInternal = @streamIdInternal
   ORDER BY dbo.Messages.Position DESC

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
            dbo.Messages.JsonData
       FROM dbo.Messages
      WHERE dbo.Messages.StreamIdInternal = @metadataStreamIdInternal
   ORDER BY dbo.Messages.Position DESC;
