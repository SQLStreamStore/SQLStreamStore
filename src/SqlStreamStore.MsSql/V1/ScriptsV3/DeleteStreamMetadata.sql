DECLARE @streamIdInternal AS INT;

SELECT @streamIdInternal = dbo.Streams.IdInternal
FROM dbo.Streams
WHERE dbo.Streams.Id = @streamId;

IF @streamIdInternal IS NOT NULL
    BEGIN
        DELETE FROM dbo.StreamsMeta
        WHERE dbo.StreamsMeta.[StreamIdInternal] = @streamIdInternal
    END