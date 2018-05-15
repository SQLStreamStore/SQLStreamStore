     SELECT TOP(@count)
            dbo.Streams.IdOriginal As StreamId,
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
      WHERE dbo.Messages.Position >= @position
   ORDER BY dbo.Messages.Position;
