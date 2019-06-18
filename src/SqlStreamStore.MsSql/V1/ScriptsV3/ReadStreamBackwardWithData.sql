    DECLARE @streamIdInternal AS INT
    DECLARE @lastStreamVersion AS INT
    DECLARE @lastStreamPosition AS BIGINT
    DECLARE @maxAge AS INT
    DECLARE @maxCount AS INT
    DECLARE @position AS BIGINT

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

     SELECT @position = dbo.Messages.Position
       FROM dbo.Messages 
      WHERE dbo.Messages.StreamIdInternal = @streamIdInternal AND dbo.Messages.StreamVersion = @streamVersion
    
    IF (@position IS NULL)
        BEGIN
            SELECT @position = MAX(dbo.Messages.Position)
              FROM dbo.Messages 
             WHERE dbo.Messages.StreamIdInternal = @streamIdInternal AND dbo.Messages.StreamVersion <= @streamVersion
        END

     SELECT TOP(@count)
            dbo.Messages.StreamVersion,
            dbo.Messages.Position,
            dbo.Messages.Id,
            dbo.Messages.Created,
            dbo.Messages.[Type],
            dbo.Messages.JsonMetadata,
            dbo.Messages.JsonData
       FROM dbo.Messages
      WHERE dbo.Messages.StreamIdInternal = @streamIdInternal AND dbo.Messages.Position <= @position
   ORDER BY dbo.Messages.Position DESC
