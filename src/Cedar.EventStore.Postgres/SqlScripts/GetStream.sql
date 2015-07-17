SELECT streams.id_internal,
       streams.is_deleted,
       events.stream_version
FROM streams
LEFT JOIN events
      ON events.stream_id_internal = streams.id_internal
WHERE streams.id = :stream_id
ORDER BY events.ordinal DESC
LIMIT 1;