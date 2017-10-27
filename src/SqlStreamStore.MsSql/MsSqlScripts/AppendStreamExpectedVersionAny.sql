DECLARE @streamIdInternal AS INT;

BEGIN TRANSACTION CreateStreamIfNotExists;
    IF NOT EXISTS (SELECT * FROM dbo.Streams WITH (UPDLOCK, ROWLOCK, HOLDLOCK)
                     WHERE dbo.Streams.Id = @streamId)
     INSERT INTO dbo.Streams (Id, IdOriginal) VALUES (@streamId, @streamIdOriginal);

COMMIT TRANSACTION CreateStreamIfNotExists;

BEGIN TRANSACTION AppendStream;
    DECLARE @latestStreamVersion AS INT;
	DECLARE @latestStreamPosition AS BIGINT;

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
            @latestStreamVersion = dbo.Messages.StreamVersion,
            @latestStreamPosition = dbo.Messages.Position
       FROM dbo.Messages
      WHERE dbo.Messages.StreamIDInternal = @streamIdInternal
   ORDER BY dbo.Messages.Position DESC

    IF @latestStreamPosition IS NULL
    SET @latestStreamPosition = -1

     UPDATE dbo.Streams
        SET dbo.Streams.[Version] = @latestStreamVersion,
            dbo.Streams.[Position] = @latestStreamPosition
      WHERE dbo.Streams.IdInternal = @streamIdInternal

COMMIT TRANSACTION AppendStream;

/* Select CurrentVersion, CurrentPosition */

    SELECT currentVersion = @latestStreamVersion, currentPosition = @latestStreamPosition

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
