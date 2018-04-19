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
  _stream_deleted  INT;
BEGIN

  SELECT current_version
  FROM __schema__.append_to_stream(
      _metadata_stream_id,
      _metadata_stream_id_original,
      _expected_version,
      _created_utc,
      ARRAY [_metadata_message]
  )
  INTO _current_version;

  SELECT COUNT(*)
  FROM __schema__.deleted_streams
  INTO _stream_deleted;

  IF (_stream_deleted = 0)
  THEN
    PERFORM __schema__.append_to_stream(
        _stream_id,
        _stream_id_original,
        -1,
        _created_utc,
        ARRAY [] :: __schema__.new_stream_message []
    );
  END IF;

  UPDATE __schema__.streams
  SET max_age = _max_age, max_count = _max_count
  WHERE __schema__.streams.id = _stream_id;

  RETURN _current_version;
END;
$F$
LANGUAGE 'plpgsql';