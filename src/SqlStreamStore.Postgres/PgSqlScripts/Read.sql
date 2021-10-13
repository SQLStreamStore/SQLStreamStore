CREATE OR REPLACE FUNCTION __schema__.read(
  _stream_id CHAR(42),
  _count     INT,
  _version   INT,
  _forwards  BOOLEAN,
  _prefetch  BOOLEAN
)
  RETURNS SETOF REFCURSOR
AS $F$
DECLARE
  _stream_id_internal INT;
  _stream_info        REFCURSOR := 'stream_info';
  _messages           REFCURSOR := 'messages';
BEGIN
  SELECT __schema__.streams.id_internal
      INTO _stream_id_internal
  FROM __schema__.streams
  WHERE __schema__.streams.id = _stream_id;

  OPEN _stream_info FOR
  SELECT __schema__.streams.version  as stream_version,
         __schema__.streams.position as position,
         __schema__.streams.max_age  as max_age
  FROM __schema__.streams
  WHERE __schema__.streams.id_internal = _stream_id_internal;

  RETURN NEXT _stream_info;

  OPEN _messages FOR
  SELECT __schema__.streams.id_original AS stream_id,
         __schema__.messages.message_id,
         __schema__.messages.stream_version,
         __schema__.messages.position,
         __schema__.messages.created_utc,
         __schema__.messages.type,
         __schema__.messages.json_metadata,
         (CASE _prefetch
            WHEN TRUE THEN __schema__.messages.json_data
            ELSE NULL END)
  FROM __schema__.messages
         INNER JOIN __schema__.streams ON __schema__.messages.stream_id_internal = __schema__.streams.id_internal
  WHERE (CASE
           WHEN _forwards THEN __schema__.messages.stream_version >= _version AND id_internal = _stream_id_internal
           ELSE __schema__.messages.stream_version <= _version AND id_internal = _stream_id_internal END)
         AND __schema__.messages.tx_id < txid_snapshot_xmin(txid_current_snapshot())
  ORDER BY (CASE
              WHEN _forwards THEN __schema__.messages.stream_version
              ELSE -__schema__.messages.stream_version END)
  LIMIT _count;

  RETURN NEXT _messages;
END;
$F$
LANGUAGE 'plpgsql';