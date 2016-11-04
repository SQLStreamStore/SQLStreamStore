/* Prefetches the Json Data */

    DECLARE @streamIdInternal AS INT
    DECLARE @lastStreamVersion AS INT

     SELECT @streamIdInternal = dbo.Streams.IdInternal, @lastStreamVersion = dbo.Streams.[Version]
       FROM dbo.Streams
      WHERE dbo.Streams.Id = @streamId

     SELECT @lastStreamVersion

     SELECT TOP(@count)
            dbo.Messages.StreamVersion,
            dbo.Messages.Position,
            dbo.Messages.Id AS EventId,
            dbo.Messages.Created,
            dbo.Messages.Type,
            dbo.Messages.JsonMetadata,
            dbo.Messages.JsonData
       FROM dbo.Messages
 INNER JOIN dbo.Streams
         ON dbo.Messages.StreamIdInternal = dbo.Streams.IdInternal
      WHERE dbo.Messages.StreamIdInternal = @streamIdInternal AND dbo.Messages.StreamVersion >= @streamVersion
   ORDER BY dbo.Messages.Position;
