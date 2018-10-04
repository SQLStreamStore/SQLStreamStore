CREATE OR REPLACE FUNCTION __schema__.list_streams_starting_with(
  _pattern           VARCHAR(1000),
  _starting_at       INT,
  _max_count         INT,
  _after_id_internal INT
)
  RETURNS TABLE(
    stream_id   VARCHAR(1000),
    id_internal INT
  ) AS $F$
BEGIN
  IF (_starting_at IS NOT NULL)
  THEN
    RETURN QUERY
    SELECT __schema__.streams.id_original, __schema__.streams.id_internal
    FROM __schema__.streams
    WHERE __schema__.streams.id_original LIKE CONCAT(_pattern, '%')
    ORDER BY __schema__.streams.id_internal ASC
    LIMIT _max_count
    OFFSET _starting_at;

  ELSEIF (_after_id_internal IS NOT NULL)
    THEN
      RETURN QUERY
      SELECT __schema__.streams.id_original, __schema__.streams.id_internal
      FROM __schema__.streams
      WHERE __schema__.streams.id_original LIKE CONCAT(_pattern, '%')
      AND __schema__.streams.id_internal > _after_id_internal
      ORDER BY __schema__.streams.id_internal ASC
      LIMIT _max_count;
  ELSE
    RAISE EXCEPTION '_starting_at and _after_id_internal are NULL';
  END IF;

END;

$F$
LANGUAGE 'plpgsql';