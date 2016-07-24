/* SQL Server 2008+ */

    DECLARE @streamIdInternal AS INT

     SELECT @streamIdInternal = dbo.Streams.IdInternal
       FROM dbo.Streams
      WHERE dbo.Streams.Id = @streamId

     SELECT TOP(@count)
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
      WHERE dbo.Events.StreamIDInternal = @streamIDInternal AND dbo.Events.StreamVersion >= @StreamVersion
   ORDER BY dbo.Events.Ordinal;

     SELECT TOP(1)
            dbo.Events.StreamVersion
       FROM dbo.Events
      WHERE dbo.Events.StreamIDInternal = @streamIDInternal
   ORDER BY dbo.Events.Ordinal DESC;