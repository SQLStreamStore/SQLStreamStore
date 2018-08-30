CREATE OR REPLACE FUNCTION __schema__.set_stream_metadata(
  _stream_id                   CHAR(42),
  _stream_id_original          VARCHAR(1000),
  _metadata_stream_id          CHAR(42),
  _metadata_stream_id_original CHAR(42),
  _max_age                     INT,
  _max_count                   INT,
  _expected_version            INT,
  _created_utc                 TIMESTAMP,
  _metadata_message            __schema__.new_stream_message)
  RETURNS INT AS $F$
DECLARE
  _current_version INT;
  _stream_updated  INT;
BEGIN
  IF _created_utc IS NULL THEN
    _created_utc = now() at time zone 'utc';
  END IF;

  SELECT current_version
  FROM __schema__.append_to_stream(
         _metadata_stream_id,
         _metadata_stream_id_original,
         _expected_version,
         _created_utc,
         ARRAY [_metadata_message]
      )
      INTO _current_version;

  UPDATE __schema__.streams
  SET max_age   = _max_age,
      max_count = _max_count
  WHERE __schema__.streams.id = _stream_id;
  GET DIAGNOSTICS _stream_updated = ROW_COUNT;

  IF (_stream_updated = 0)
  THEN
    PERFORM __schema__.append_to_stream(
              _stream_id,
              _stream_id_original,
              -1,
              _created_utc,
              ARRAY [] :: __schema__.new_stream_message []);
    UPDATE __schema__.streams
    SET max_age   = _max_age,
        max_count = _max_count
    WHERE __schema__.streams.id = _stream_id;
  END IF;

  RETURN _current_version;
END;
$F$
LANGUAGE 'plpgsql';