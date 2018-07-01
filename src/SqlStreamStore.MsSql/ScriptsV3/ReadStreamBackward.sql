    DECLARE @streamIdInternal AS INT
    DECLARE @lastStreamVersion AS INT
    DECLARE @lastStreamPosition AS BIGINT
    DECLARE @maxAge as INT
    DECLARE @maxCount as INT

     SELECT @streamIdInternal = dbo.Streams.IdInternal,
            @lastStreamVersion = dbo.Streams.[Version],
            @lastStreamPosition = dbo.Streams.[Position],
            @maxAge = dbo.Streams.[MaxAge],
            @maxCount = dbo.Streams.[MaxCount]
       FROM dbo.Streams
      WHERE dbo.Streams.Id = @streamId

     SELECT @lastStreamVersion,
            @lastStreamPosition,
            @maxAge,
            @maxCount

     SELECT TOP(@count)
            dbo.Messages.StreamVersion,
            dbo.Messages.Position,
            dbo.Messages.Id,
            dbo.Messages.Created,
            dbo.Messages.[Type],
            dbo.Messages.JsonMetadata
       FROM dbo.Messages
      WHERE dbo.Messages.StreamIdInternal = @streamIdInternal AND dbo.Messages.StreamVersion <= @streamVersion
   ORDER BY dbo.Messages.Position DESC
