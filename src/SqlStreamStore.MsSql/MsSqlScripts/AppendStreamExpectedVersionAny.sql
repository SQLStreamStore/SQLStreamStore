DECLARE @streamIdInternal AS INT;

BEGIN TRANSACTION CreateStreamIfNotExists;

    IF NOT EXISTS (
        SELECT *
        FROM dbo.Streams WITH (UPDLOCK, ROWLOCK, HOLDLOCK)
        WHERE dbo.Streams.Id = @streamId
    )
        INSERT INTO dbo.Streams (Id, IdOriginal)
        VALUES (@streamId, @streamIdOriginal);

COMMIT TRANSACTION CreateStreamIfNotExists;

BEGIN TRANSACTION AppendStream;

    DECLARE @latestStreamVersion AS INT;
    DECLARE @latestStreamPosition AS BIGINT;

    SELECT @streamIdInternal = dbo.Streams.IdInternal, @latestStreamVersion = dbo.Streams.[Version]
    FROM dbo.Streams WITH (UPDLOCK, ROWLOCK)
    WHERE dbo.Streams.Id = @streamId;

    INSERT INTO dbo.Messages
        (StreamIdInternal, StreamVersion, Id, Created, [Type], JsonData, JsonMetadata)
        SELECT @streamIdInternal, StreamVersion + @latestStreamVersion + 1, Id, Created, [Type], JsonData, JsonMetadata
        FROM @newMessages
        ORDER BY StreamVersion

    SET @latestStreamPosition = ISNULL(SCOPE_IDENTITY(), -1)

    SELECT @latestStreamVersion = MAX(StreamVersion) + @latestStreamVersion + 1
    FROM @newMessages

    SET @latestStreamVersion = ISNULL(@latestStreamVersion, -1)

    UPDATE dbo.Streams
        SET dbo.Streams.[Version] = @latestStreamVersion,
            dbo.Streams.[Position] = @latestStreamPosition
        WHERE dbo.Streams.IdInternal = @streamIdInternal

COMMIT TRANSACTION AppendStream;

/* Select CurrentVersion, CurrentPosition */

SELECT currentVersion = @latestStreamVersion, currentPosition = @latestStreamPosition

/* Select Metadata */

SELECT dbo.Messages.JsonData
FROM dbo.Messages
WHERE dbo.Messages.Position = (
    SELECT dbo.Streams.Position
    FROM dbo.Streams
    WHERE dbo.Streams.Id = '$$' + @streamId
)