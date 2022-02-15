SET TimeZone='UTC';
ALTER TABLE __schema__.messages ALTER COLUMN "created_utc" TYPE TIMESTAMP WITH TIME ZONE;
COMMENT ON SCHEMA __schema__ IS '{ "version": 2 }';