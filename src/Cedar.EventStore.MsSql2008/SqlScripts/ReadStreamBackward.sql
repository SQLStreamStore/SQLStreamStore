/* SQL Server 2008+ */

    DECLARE @streamIdInternal AS INT
    DECLARE @isDeleted AS BIT

     SELECT @streamIdInternal = dbo.Streams.IdInternal,
            @isDeleted = dbo.Streams.IsDeleted
       FROM dbo.Streams
      WHERE dbo.Streams.Id = @streamId

     SELECT @isDeleted;

     SELECT TOP(@count)
            dbo.Events.StreamVersion,
            dbo.Events.Ordinal,
            dbo.Events.Id AS EventId,
            dbo.Events.Created,
            dbo.Events.Type,
            dbo.Events.JsonData,
            dbo.Events.JsonMetadata
       FROM dbo.Events
 INNER JOIN Streams
         ON dbo.Events.StreamIdInternal = dbo.Streams.IdInternal
      WHERE dbo.Events.StreamIDInternal = @streamIDInternal AND dbo.Events.StreamVersion <= @StreamVersion
   ORDER BY dbo.Events.Ordinal DESC

     SELECT TOP(1)
            dbo.Events.StreamVersion
       FROM dbo.Events
      WHERE dbo.Events.StreamIDInternal = @streamIDInternal
   ORDER BY dbo.Events.Ordinal DESC;