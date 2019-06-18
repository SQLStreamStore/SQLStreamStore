-- If the stream exists, lift the maxCount and maxAge metadata to the streams table

IF EXISTS (
    SELECT *
    FROM dbo.Streams WITH (UPDLOCK, ROWLOCK, HOLDLOCK)
    WHERE dbo.Streams.Id = @streamId
)
BEGIN
    UPDATE dbo.Streams
    SET dbo.Streams.[MaxAge] = @maxAge,
        dbo.Streams.[MaxCount] = @maxCount
    WHERE dbo.Streams.[Id] = @streamId
END