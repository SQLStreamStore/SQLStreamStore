BEGIN TRANSACTION AppendStream;

    DECLARE @streamIdInternal AS INT;
    DECLARE @latestStreamVersion AS INT;
    DECLARE @latestStreamPosition AS BIGINT;
    DECLARE @headPosition AS BIGINT;

    SELECT @streamIdInternal = dbo.Streams.IdInternal, @latestStreamVersion = dbo.Streams.[Version]
    FROM dbo.Streams WITH (UPDLOCK, ROWLOCK)
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
    
    SELECT @headPosition = ISNULL(MAX(Position), -1) FROM dbo.Messages

    INSERT INTO dbo.Messages
        (Position, StreamIdInternal, StreamVersion, Id, Created, [Type], JsonData, JsonMetadata)
        SELECT @headPosition + StreamVersion + 1, @streamIdInternal, StreamVersion + @latestStreamVersion + 1, Id, Created, [Type], JsonData, JsonMetadata
        FROM @newMessages
        ORDER BY StreamVersion;

    SELECT @latestStreamPosition = MAX(StreamVersion) + @headPosition + 1
    FROM @newMessages

    SELECT @latestStreamVersion = MAX(StreamVersion) + @latestStreamVersion + 1
    FROM @newMessages

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
