CREATE OR REPLACE FUNCTION __schema__.listen()
  RETURNS VOID AS $F$
  BEGIN
    LISTEN on_append___schema__;
  END;
$F$
LANGUAGE 'plpgsql';