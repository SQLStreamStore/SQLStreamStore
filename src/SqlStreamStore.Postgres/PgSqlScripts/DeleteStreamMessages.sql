CREATE OR REPLACE FUNCTION __schema__.delete_stream_messages(
  _stream_id                  CHAR(42),
  _message_ids                UUID [],
  _deleted_stream_id          CHAR(42),
  _deleted_stream_id_original VARCHAR(1000),
  _created_utc                TIMESTAMP,
  _deleted_messages           __schema__.new_stream_message []
)
  RETURNS VOID
AS $F$
DECLARE
  _stream_id_internal INT;
  _deleted_count      NUMERIC;
BEGIN
  IF _created_utc IS NULL THEN
    _created_utc = now() at time zone 'utc';
  END IF;

  SELECT __schema__.streams.id_internal
      INTO _stream_id_internal
  FROM __schema__.streams
  WHERE __schema__.streams.id = _stream_id;

  WITH deleted AS (DELETE FROM __schema__.messages
  WHERE __schema__.messages.stream_id_internal = _stream_id_internal
        AND __schema__.messages.message_id = ANY (_message_ids)
  RETURNING *)
  SELECT count(*)
  FROM deleted
      INTO _deleted_count;

  IF _deleted_count > 0
  THEN
    PERFORM __schema__.append_to_stream(
              _deleted_stream_id,
              _deleted_stream_id_original,
              -2,
              _created_utc,
              _deleted_messages);
  END IF;
END;

$F$
LANGUAGE 'plpgsql';