DECLARE @streamIdInternal AS INT;
DECLARE @latestStreamVersion AS INT;
DECLARE @latestStreamPosition AS BIGINT;
DECLARE @maxCount as INT;

BEGIN TRANSACTION CreateStream;

    BEGIN

        INSERT INTO dbo.Streams
            (Id, IdOriginal)
        VALUES
            (@streamId, @streamIdOriginal);

        SET @streamIdInternal = SCOPE_IDENTITY();

        IF @hasMessages = 1
            BEGIN
                INSERT INTO dbo.Messages
                    (StreamIdInternal, StreamVersion, Id, Created, [Type], JsonData, JsonMetadata)
                    SELECT @streamIdInternal, StreamVersion, Id, Created, [Type], JsonData, JsonMetadata
                    FROM @newMessages
                    ORDER BY StreamVersion;

                SET @latestStreamPosition = SCOPE_IDENTITY()

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

   SELECT currentVersion = @latestStreamVersion,
          currentPosition = @latestStreamPosition,
          maxCount = dbo.Streams.MaxCount
     FROM dbo.Streams
    WHERE dbo.Streams.IdInternal = @streamIdInternal;