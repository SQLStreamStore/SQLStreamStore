/* SQL Server 2008+ */

    DECLARE @streamIdInternal AS INT

     SELECT @streamIdInternal = Streams.IdInternal
       FROM Streams
      WHERE Streams.Id = @streamId

     SELECT TOP(@count)
            Streams.IdOriginal As StreamId,
            Streams.IsDeleted as IsDeleted,
            Events.StreamRevision,
            Events.Ordinal,
            Events.Id AS EventId,
            Events.Created,
            Events.Type,
            Events.JsonData,
            Events.JsonMetadata
       FROM Events
 INNER JOIN Streams
         ON Events.StreamIdInternal=Streams.IdInternal
      WHERE Events.StreamIDInternal = @streamIDInternal AND Events.Ordinal >= @ordinal
   ORDER BY Events.Ordinal DESC