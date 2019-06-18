DECLARE @streamIdInternal AS INT;
DECLARE @latestStreamVersion AS INT;
DECLARE @latestStreamPosition AS BIGINT;
DECLARE @maxCount as INT;

BEGIN TRANSACTION AppendStream;

    SELECT @streamIdInternal = dbo.Streams.IdInternal,
           @latestStreamVersion = dbo.Streams.[Version]
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

    INSERT INTO dbo.Messages
        (StreamIdInternal, StreamVersion, Id, Created, [Type], JsonData, JsonMetadata)
        SELECT @streamIdInternal, StreamVersion + @latestStreamVersion + 1, Id, Created, [Type], JsonData, JsonMetadata
        FROM @newMessages
        ORDER BY StreamVersion;

    SET @latestStreamPosition = SCOPE_IDENTITY()

    SELECT @latestStreamVersion = MAX(StreamVersion) + @latestStreamVersion + 1
    FROM @newMessages

    UPDATE dbo.Streams
        SET dbo.Streams.[Version] = @latestStreamVersion,
            dbo.Streams.[Position] = @latestStreamPosition
      WHERE dbo.Streams.IdInternal = @streamIdInternal

COMMIT TRANSACTION AppendStream;

   SELECT currentVersion = @latestStreamVersion,
          currentPosition = @latestStreamPosition,
          maxCount = dbo.Streams.MaxCount
     FROM dbo.Streams
    WHERE dbo.Streams.IdInternal = @streamIdInternal;
