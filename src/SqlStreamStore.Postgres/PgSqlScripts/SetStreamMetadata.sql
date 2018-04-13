CREATE OR REPLACE FUNCTION public.set_stream_metadata(
  _stream_id                   CHAR(42),
  _metadata_stream_id          CHAR(42),
  _metadata_stream_id_original CHAR(42),
  _max_age                     INT,
  _max_count                   INT,
  _expected_version            INT,
  _created_utc                 TIMESTAMP,
  _metadata_message            public.new_stream_message)
  RETURNS INT AS $F$
DECLARE
  _current_version INT;
BEGIN

  SELECT current_version
  FROM public.append_to_stream(
      _metadata_stream_id,
      _metadata_stream_id_original,
      _expected_version,
      _created_utc,
      ARRAY [_metadata_message]
  )
  INTO _current_version;

  UPDATE public.streams
  SET max_age = _max_age, max_count = _max_count
  WHERE public.streams.id = _stream_id;
  
  RETURN _current_version;
END;
$F$
LANGUAGE 'plpgsql';