    DECLARE @streamIdInternal AS INT

     SELECT @streamIdInternal = dbo.Streams.IdInternal
       FROM dbo.Streams
      WHERE dbo.Streams.Id = @streamId

     SELECT COUNT (*)
       FROM dbo.Events
      WHERE dbo.Events.StreamIdInternal = @streamIdInternal