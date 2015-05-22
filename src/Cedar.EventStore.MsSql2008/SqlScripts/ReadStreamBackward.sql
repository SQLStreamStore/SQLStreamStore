/* SQL Server 2008+ */

    DECLARE @streamIdInternal AS INT
    DECLARE @isDeleted AS BIT

     SELECT @streamIdInternal = Streams.IdInternal,
            @isDeleted = Streams.IsDeleted
       FROM Streams
      WHERE Streams.Id = @streamId

     SELECT @isDeleted;

     SELECT TOP(@count)
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
      WHERE Events.StreamIDInternal = @streamIDInternal AND Events.StreamVersion <= @StreamVersion
   ORDER BY Events.Ordinal DESC

     SELECT TOP(1)
            Events.StreamVersion
       FROM Events
      WHERE Events.StreamIDInternal = @streamIDInternal
   ORDER BY Events.Ordinal DESC;