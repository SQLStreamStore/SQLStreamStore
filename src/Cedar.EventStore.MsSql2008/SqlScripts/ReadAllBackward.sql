/* SQL Server 2008+ */
     SELECT TOP(@count)
            dbo.Streams.IdOriginal As StreamId,
            dbo.Events.StreamVersion,
            dbo.Events.Ordinal,
            dbo.Events.Id AS EventId,
            dbo.Events.Created,
            dbo.Events.Type,
            dbo.Events.JsonData,
            dbo.Events.JsonMetadata
       FROM dbo.Events
 INNER JOIN dbo.Streams
         ON dbo.Events.StreamIdInternal = dbo.Streams.IdInternal
      WHERE dbo.Events.Ordinal <= @ordinal
   ORDER BY dbo.Events.Ordinal DESC;