    DECLARE @streamIdInternal AS INT

     SELECT @streamIdInternal = dbo.Streams.IdInternal
       FROM dbo.Streams
      WHERE dbo.Streams.Id = @streamId

     SELECT dbo.Messages.JsonData
       FROM dbo.Messages
      WHERE dbo.Messages.StreamIdInternal = @streamIdInternal AND dbo.Messages.StreamVersion = @streamVersion;
