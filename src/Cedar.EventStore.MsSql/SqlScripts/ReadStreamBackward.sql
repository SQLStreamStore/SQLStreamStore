/* SQL Server 2008+ */

    DECLARE @streamIdInternal AS INT
    DECLARE @isDeleted AS BIT

     SELECT @streamIdInternal = Streams.IdInternal,
            @isDeleted = Streams.IsDeleted
       FROM Streams
      WHERE Streams.Id = @streamId

     SELECT @isDeleted;

     SELECT TOP(@count)
            Events.StreamRevision,
            Events.Ordinal,
            Events.Id AS EventId,
            Events.Created,
            Events.Type,
            Events.JsonData,
            Events.JsonMetadata
       FROM Events
 INNER JOIN Streams
         ON Events.StreamIdInternal = Streams.IdInternal
      WHERE Events.StreamIDInternal = @streamIDInternal AND Events.StreamRevision <= @streamRevision
   ORDER BY Events.Ordinal DESC

     SELECT TOP(1)
            Events.StreamRevision
       FROM Events
      WHERE Events.StreamIDInternal = @streamIDInternal
   ORDER BY Events.Ordinal DESC;