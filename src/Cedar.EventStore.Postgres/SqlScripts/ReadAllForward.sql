SELECT 
            streams.id_original As stream_id,
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
      WHERE events.ordinal >= :ordinal
   ORDER BY events.ordinal
LIMIT :count