/* SQL Server 2008+ */
     SELECT TOP(@count)
            Streams.IdOriginal As StreamId,
            Events.StreamVersion,
            Events.Ordinal,
            Events.Id AS EventId,
            Events.Created,
            Events.Type,
            Events.JsonData,
            Events.JsonMetadata
       FROM Events
 INNER JOIN Streams
         ON Events.StreamIdInternal = Streams.IdInternal
      WHERE Events.Ordinal >= @ordinal
   ORDER BY Events.Ordinal;