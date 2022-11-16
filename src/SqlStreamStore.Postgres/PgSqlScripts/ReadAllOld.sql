CREATE OR REPLACE FUNCTION __schema__.read_all2(
  _count    INT,
  _position BIGINT,
  _forwards BOOLEAN,
  _prefetch BOOLEAN
)
  RETURNS SETOF REFCURSOR

AS $F$

DECLARE
  _messages           REFCURSOR := 'messages';
  _txinfo             REFCURSOR := 'tx_info';
BEGIN

  OPEN _messages FOR
  WITH messages AS (
      SELECT __schema__.streams.id_original,
             __schema__.messages.message_id,
             __schema__.messages.stream_version,
             __schema__.messages.position,
             __schema__.messages.created_utc,
             __schema__.messages.type,
             __schema__.messages.json_metadata,
             (CASE _prefetch
                WHEN TRUE THEN __schema__.messages.json_data
                ELSE NULL END),
             __schema__.streams.max_age
      FROM __schema__.messages
             INNER JOIN __schema__.streams ON __schema__.messages.stream_id_internal = __schema__.streams.id_internal
      WHERE (CASE
               WHEN _forwards THEN __schema__.messages.position >= _position
               ELSE __schema__.messages.position <= _position END)
      ORDER BY
          (CASE WHEN _forwards THEN __schema__.messages.position END),
          (CASE WHEN not _forwards THEN __schema__.messages.position END) DESC
      LIMIT _count
  )
  SELECT * FROM messages LIMIT _count;

  RETURN NEXT _messages;

  OPEN _txinfo FOR	
  SELECT txid_snapshot_xip(txid_current_snapshot());
  RETURN NEXT _txinfo;

END;
$F$
LANGUAGE 'plpgsql';