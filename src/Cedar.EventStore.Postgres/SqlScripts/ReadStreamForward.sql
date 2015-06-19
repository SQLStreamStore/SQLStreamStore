/* SQL Server 2008+ */

    DECLARE @streamIdInternal AS INT
    DECLARE @isDeleted AS BIT

     SELECT @streamIdInternal = Streams.IdInternal,
            @isDeleted = Streams.IsDeleted
       FROM Streams
      WHERE Streams.Id = @streamId

     SELECT @isDeleted;

     SELECT 
            events.stream_version,
            events.ordinal,
            events.id AS event_id,
            events.created,
            events.type,
            events.json_data,
            events.json_metadata
       FROM events
      INNER JOIN streams
         ON events.stream_id_internal = streams.id_internal
      WHERE events.stream_id_internal = :stream_id_internal AND events.stream_version >= :stream_version
   ORDER BY events.Ordinal
   LIMIT :count;

     SELECT events.StreamVersion
       FROM events
      WHERE events.StreamIDInternal = @streamIDInternal
   ORDER BY events.Ordinal DESC
   LIMIT 1;