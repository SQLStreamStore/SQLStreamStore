CREATE OR REPLACE FUNCTION __schema__.read_schema_version()
  RETURNS INT
AS $F$
BEGIN

  RETURN (
    SELECT obj_description :: JSON -> 'version'
    FROM obj_description('__schema__' :: REGNAMESPACE, 'pg_namespace')
  );

END;
$F$
LANGUAGE 'plpgsql';