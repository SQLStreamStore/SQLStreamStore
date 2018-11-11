    DECLARE @streamIdInternal AS INT
    DECLARE @lastStreamVersion AS INT
	DECLARE @lastStreamPosition AS BIGINT
    DECLARE @position AS BIGINT

     SELECT @streamIdInternal = dbo.Streams.IdInternal, @lastStreamVersion = dbo.Streams.[Version], @lastStreamPosition = dbo.Streams.[Position]
       FROM dbo.Streams
      WHERE dbo.Streams.Id = @streamId

     SELECT @lastStreamVersion, @lastStreamPosition

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
            dbo.Messages.Id AS EventId,
            dbo.Messages.Created,
            dbo.Messages.Type,
            dbo.Messages.JsonMetadata,
            dbo.Messages.JsonData
       FROM dbo.Messages
      WHERE dbo.Messages.StreamIdInternal = @streamIdInternal AND dbo.Messages.Position <= @position
   ORDER BY dbo.Messages.Position DESC
