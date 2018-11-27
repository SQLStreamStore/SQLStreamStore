CREATE OR REPLACE FUNCTION __schema__.list_streams(
  _max_count         INT,
  _after_id_internal INT
)
  RETURNS TABLE(
    stream_id   VARCHAR(1000),
    id_internal INT
  ) AS $F$
BEGIN
  RETURN QUERY
  SELECT __schema__.streams.id_original, __schema__.streams.id_internal
  FROM __schema__.streams
  WHERE __schema__.streams.id_internal > _after_id_internal
  ORDER BY __schema__.streams.id_internal ASC
  LIMIT _max_count;
END;

$F$
LANGUAGE 'plpgsql';