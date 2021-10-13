CREATE OR REPLACE FUNCTION __schema__.read_all2(
  _count    INT,
  _position BIGINT,
  _forwards BOOLEAN,
  _prefetch BOOLEAN,
  _max_position BIGINT
)
  -- RETURNS TABLE(
  --   stream_id      VARCHAR(1000),
  --   message_id     UUID,
  --   stream_version INT,
  --   "position"     BIGINT,
  --   create_utc     TIMESTAMP,
  --   "type"         VARCHAR(128),
  --   json_metadata  JSONB,
  --   json_data      JSONB,
  --   max_age        INT
  -- )
  RETURNS SETOF REFCURSOR

AS $F$

DECLARE
  -- _stream_id_internal INT;
  -- _stream_info        REFCURSOR := 'stream_info';
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
               WHEN _forwards AND _max_position > 0 THEN __schema__.messages.position >= _position AND __schema__.messages.position <= _max_position
               WHEN _forwards THEN __schema__.messages.position >= _position
               ELSE __schema__.messages.position <= _position END)
            AND __schema__.messages.tx_id < txid_snapshot_xmin(txid_current_snapshot())
      ORDER BY
          (CASE WHEN _forwards THEN __schema__.messages.position END),
          (CASE WHEN not _forwards THEN __schema__.messages.position END) DESC
      LIMIT _count
  )
  SELECT * FROM messages LIMIT _count;

  RETURN NEXT _messages;

  OPEN _txinfo FOR
  SELECT txid_current_snapshot()::TEXT;
  RETURN NEXT _txinfo;
END;
$F$
LANGUAGE 'plpgsql';