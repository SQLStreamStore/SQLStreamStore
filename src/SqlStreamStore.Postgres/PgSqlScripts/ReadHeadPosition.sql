CREATE OR REPLACE FUNCTION __schema__.read_head_position()
  RETURNS BIGINT
AS $F$
BEGIN
  RETURN (
    SELECT max(__schema__.messages.position)
    FROM __schema__.messages
  );
END;
$F$
LANGUAGE 'plpgsql';