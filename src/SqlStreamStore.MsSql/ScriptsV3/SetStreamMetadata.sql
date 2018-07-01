IF NOT EXISTS (
    SELECT *
    FROM dbo.Streams WITH (UPDLOCK, ROWLOCK, HOLDLOCK)
    WHERE dbo.Streams.Id = @streamId
)
BEGIN
    INSERT INTO dbo.Streams (Id, IdOriginal, MaxAge, MaxCount)
    VALUES (@streamId, @streamIdOriginal, @maxAge, @maxCount);
END
ELSE
BEGIN
    UPDATE dbo.Streams
    SET dbo.Streams.[MaxAge] = @maxAge,
        dbo.Streams.[MaxCount] = @maxCount
    WHERE dbo.Streams.[Id] = @streamId
END