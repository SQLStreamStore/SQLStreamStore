     SELECT Streams.IdOriginal As StreamId,
            Messages.StreamVersion,
            Messages.Position,
            Messages.Id AS EventId,
            Messages.Created,
            Messages.Type,
            Messages.JsonMetadata,
            Messages.JsonData
       FROM Messages
 INNER JOIN Streams
         ON Messages.StreamIdInternal = Streams.IdInternal
      WHERE Messages.Position <= @ordinal
   ORDER BY Messages.Position DESC
      LIMIT ?count;
