BEGIN TRANSACTION CreateStream;

    DECLARE @streamIdInternal AS INT;
    DECLARE @latestStreamVersion AS INT;
    DECLARE @latestStreamPosition AS BIGINT;
    DECLARE @headPosition AS BIGINT;

    BEGIN

        INSERT INTO dbo.Streams
            (Id, IdOriginal)
        VALUES
            (@streamId, @streamIdOriginal);

        SET @streamIdInternal = SCOPE_IDENTITY();

        IF @hasMessages = 1
            BEGIN
                
                SELECT @headPosition = ISNULL(MAX(Position), -1) FROM dbo.Messages

                INSERT INTO dbo.Messages
                    (Position, StreamIdInternal, StreamVersion, Id, Created, [Type], JsonData, JsonMetadata)
                    SELECT @headPosition + StreamVersion + 1, @streamIdInternal, StreamVersion, Id, Created, [Type], JsonData, JsonMetadata
                    FROM @newMessages
                    ORDER BY StreamVersion;

                SELECT @latestStreamPosition = MAX(StreamVersion) + @headPosition + 1
                FROM @newMessages

                SELECT @latestStreamVersion = MAX(StreamVersion)
                FROM @newMessages

                UPDATE dbo.Streams
                    SET dbo.Streams.[Version] = @latestStreamVersion,
                        dbo.Streams.[Position] = @latestStreamPosition
                    WHERE dbo.Streams.IdInternal = @streamIdInternal
            END
        ELSE
            BEGIN
                SET @latestStreamPosition = -1
                SET @latestStreamVersion = -1
            END

    END;

COMMIT TRANSACTION CreateStream;

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