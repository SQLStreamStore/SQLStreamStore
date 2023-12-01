CREATE OR REPLACE FUNCTION __schema__.read_xmin()
  RETURNS XID8
AS $F$
BEGIN
  RETURN (SELECT pg_snapshot_xmin(pg_current_snapshot()));
END;
$F$
LANGUAGE 'plpgsql';