BEGIN TRANSACTION SetStreamMetadata;

    DECLARE @streamIdInternal AS INT;

    SELECT @streamIdInternal = dbo.Streams.IdInternal
    FROM dbo.Streams WITH (UPDLOCK, ROWLOCK)
    WHERE dbo.Streams.Id = @streamId;

    IF @streamIdInternal IS NOT NULL
        BEGIN
            UPDATE dbo.Streams 
            SET dbo.Streams.[MaxAge] = @maxAge,
                dbo.Streams.[MaxCount] = @maxCount
        END

COMMIT TRANSACTION SetStreamMetadata;