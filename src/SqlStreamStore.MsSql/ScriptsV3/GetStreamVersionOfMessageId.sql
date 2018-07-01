    DECLARE @streamIdInternal AS INT

     SELECT @streamIdInternal = dbo.Streams.IdInternal
       FROM dbo.Streams
      WHERE dbo.Streams.Id = @streamId

     SELECT TOP 1 dbo.Messages.[StreamVersion]
       FROM dbo.Messages
      WHERE dbo.Messages.StreamIdInternal = @streamIdInternal AND dbo.Messages.Id = @messageId
