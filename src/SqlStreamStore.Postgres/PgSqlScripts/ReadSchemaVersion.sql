CREATE OR REPLACE FUNCTION public.read_schema_version()
  RETURNS INT
AS $F$
BEGIN

  RETURN (
    SELECT obj_description :: JSON -> 'version'
    FROM obj_description('public.streams' :: REGCLASS, 'pg_class')
  );

END;
$F$
LANGUAGE 'plpgsql';