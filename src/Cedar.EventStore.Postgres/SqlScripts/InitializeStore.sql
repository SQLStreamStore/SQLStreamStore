

CREATE SCHEMA IF NOT EXISTS $schema$;

CREATE TABLE $schema$.streams(
    id_internal SERIAL PRIMARY KEY,
    id text NOT NULL,
    id_original text NOT NULL,
    is_deleted boolean DEFAULT (false) NOT NULL
);

CREATE UNIQUE INDEX ix_streams_id
ON $schema$.streams
USING btree(id);

CREATE TABLE $schema$.events(
    stream_id_internal integer NOT NULL,
    stream_version integer NOT NULL,
    ordinal SERIAL PRIMARY KEY NOT NULL ,
    id uuid NOT NULL,
    created timestamp NOT NULL,
    type text NOT NULL,
    json_data json NOT NULL,
    json_metadata json ,
    CONSTRAINT fk_events_streams FOREIGN KEY (stream_id_internal) REFERENCES $schema$.streams(id_internal)
);

CREATE UNIQUE INDEX ix_events_stream_id_internal_revision
ON $schema$.events
USING btree(stream_id_internal, stream_version DESC, ordinal DESC);

CREATE OR REPLACE FUNCTION $schema$.create_stream(_stream_id text, _stream_id_original text)
RETURNS integer AS
$BODY$
DECLARE
    _result integer;
BEGIN
    INSERT INTO $schema$.streams(id, id_original)
    VALUES (_stream_id, _stream_id_original)
    RETURNING id_internal
    INTO _result;

    RETURN _result;
END;
$BODY$
LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION $schema$.get_stream(_stream_id text)
RETURNS TABLE(id_internal integer, is_deleted boolean, stream_version integer) AS
$BODY$
BEGIN
    RETURN QUERY
    SELECT $schema$.streams.id_internal,
           $schema$.streams.is_deleted,
           (SELECT max($schema$.events.stream_version) from $schema$.events where $schema$.events.stream_id_internal = $schema$.streams.id_internal)
    FROM $schema$.streams
    WHERE $schema$.streams.id = _stream_id;
END;
$BODY$
LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION $schema$.read_all_forward(_ordinal bigint, _count integer)
RETURNS TABLE(stream_id integer, stream_version integer, ordinal bigint, event_id uuid, created timestamp, type text, json_data json, json_metadata json) AS
$BODY$
BEGIN
    RETURN QUERY
    SELECT 
                $schema$.streams.id_original As stream_id,
                $schema$.events.stream_version,
                $schema$.events.ordinal,
                $schema$.events.id AS event_id,
                $schema$.events.created,
                $schema$.events.type,
                $schema$.events.json_data,
                $schema$.events.json_metadata
           FROM $schema$.events
     INNER JOIN $schema$.streams
             ON $schema$.events.stream_id_internal = $schema$.streams.id_internal
          WHERE $schema$.events.ordinal >= _ordinal
       ORDER BY $schema$.events.ordinal
    LIMIT _count;
END;
$BODY$
LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION $schema$.read_all_backward(_ordinal bigint, _count integer)
RETURNS TABLE(stream_id integer, stream_version integer, ordinal bigint, event_id uuid, created timestamp, type text, json_data json, json_metadata json) AS
$BODY$
BEGIN
    RETURN QUERY
    SELECT 
                $schema$.streams.id_original As stream_id,
                $schema$.events.stream_version,
                $schema$.events.ordinal,
                $schema$.events.id AS event_id,
                $schema$.events.created,
                $schema$.events.type,
                $schema$.events.json_data,
                $schema$.events.json_metadata
           FROM $schema$.events
     INNER JOIN $schema$.streams
             ON $schema$.events.stream_id_internal = $schema$.streams.id_internal
          WHERE $schema$.events.ordinal <= _ordinal
       ORDER BY $schema$.events.ordinal DESC
    LIMIT _count;
END;
$BODY$
LANGUAGE plpgsql;


CREATE OR REPLACE FUNCTION $schema$.read_stream_forward(_stream_id text, _count integer, _stream_version integer) RETURNS SETOF refcursor AS
$BODY$
DECLARE 
  ref1 refcursor;
  ref2 refcursor;
  ref3 refcursor;  
  _stream_id_internal integer;
  _is_deleted boolean;
BEGIN
SELECT $schema$.streams.id_internal, $schema$.streams.is_deleted
 INTO _stream_id_internal, _is_deleted
 FROM $schema$.streams
 WHERE $schema$.streams.id = _stream_id;
 
OPEN ref1 FOR 
 SELECT _stream_id_internal, _is_deleted;
RETURN NEXT ref1;


OPEN ref2 FOR 
     SELECT 
            $schema$.events.stream_version,
            $schema$.events.ordinal,
            $schema$.events.id AS event_id,
            $schema$.events.created,
            $schema$.events.type,
            $schema$.events.json_data,
            $schema$.events.json_metadata
       FROM $schema$.events
      INNER JOIN $schema$.streams
         ON $schema$.events.stream_id_internal = $schema$.streams.id_internal
      WHERE $schema$.events.stream_id_internal = _stream_id_internal
      AND   $schema$.events.stream_version >= _stream_version
      ORDER BY $schema$.events.ordinal
      LIMIT _count;
RETURN next ref2;

OPEN ref3 FOR 
 SELECT $schema$.events.stream_version
       FROM $schema$.events
      WHERE $schema$.events.stream_id_internal = _stream_id_internal
   ORDER BY $schema$.events.ordinal DESC
   LIMIT 1;
RETURN next ref3;

RETURN;
END;
$BODY$
LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION $schema$.read_stream_backward(_stream_id text, _count integer, _stream_version integer) RETURNS SETOF refcursor AS
$BODY$
DECLARE 
  ref1 refcursor;
  ref2 refcursor;
  ref3 refcursor;  
  _stream_id_internal integer;
  _is_deleted boolean;
BEGIN

SELECT $schema$.streams.id_internal, $schema$.streams.is_deleted
 INTO _stream_id_internal, _is_deleted
 FROM $schema$.streams
 WHERE $schema$.streams.id = _stream_id;
 
OPEN ref1 FOR 
 SELECT _stream_id_internal, _is_deleted;
RETURN NEXT ref1;


OPEN ref2 FOR 
     SELECT 
            $schema$.events.stream_version,
            $schema$.events.ordinal,
            $schema$.events.id AS event_id,
            $schema$.events.created,
            $schema$.events.type,
            $schema$.events.json_data,
            $schema$.events.json_metadata
       FROM $schema$.events
      INNER JOIN $schema$.streams
         ON $schema$.events.stream_id_internal = $schema$.streams.id_internal
      WHERE $schema$.events.stream_id_internal = _stream_id_internal
      AND   $schema$.events.stream_version <= _stream_version
      ORDER BY $schema$.events.ordinal DESC
      LIMIT _count;
RETURN next ref2;

OPEN ref3 FOR 
 SELECT $schema$.events.stream_version
       FROM $schema$.events
      WHERE $schema$.events.stream_id_internal = _stream_id_internal
   ORDER BY $schema$.events.ordinal DESC
   LIMIT 1;
RETURN next ref3;

RETURN;
END;
$BODY$
LANGUAGE plpgsql;


CREATE OR REPLACE FUNCTION $schema$.delete_stream_any_version(stream_id text) RETURNS VOID AS
$BODY$
DECLARE 
  _stream_id_internal integer;
BEGIN

 SELECT $schema$.streams.id_internal
 INTO _stream_id_internal
 FROM $schema$.streams
 WHERE $schema$.streams.id = stream_id;

 DELETE FROM $schema$.events
 WHERE $schema$.events.stream_id_internal = _stream_id_internal;

 UPDATE $schema$.streams
 SET is_deleted = true
 WHERE $schema$.streams.id_internal = _stream_id_internal;
 
RETURN;
END;
$BODY$
LANGUAGE plpgsql;


CREATE OR REPLACE FUNCTION $schema$.delete_stream_expected_version(stream_id text, expected_version integer) RETURNS VOID AS
$BODY$
DECLARE 
  _stream_id_internal integer;
  _lastest_stream_version integer;
BEGIN

 SELECT $schema$.streams.id_internal
 INTO _stream_id_internal
 FROM $schema$.streams
 WHERE $schema$.streams.id = stream_id;

 IF _stream_id_internal IS NULL THEN
    RAISE EXCEPTION  'WrongExpectedVersion'
    USING HINT = 'The Stream ' || stream_id || ' does not exist.';
 END IF;

 SELECT stream_version
 INTO _lastest_stream_version
 FROM $schema$.events
 WHERE $schema$.events.stream_id_internal = _stream_id_internal
 ORDER BY $schema$.events.ordinal DESC
 LIMIT 1; 

 IF (_lastest_stream_version <> expected_version) THEN
    RAISE EXCEPTION  'WrongExpectedVersion'
    USING HINT = 'The Stream ' || stream_id || 'version was expected to be' || expected_version::text || ' but was version ' || _lastest_stream_version::text || '.' ;
 END IF;

 DELETE FROM $schema$.events
 WHERE $schema$.events.stream_id_internal = _stream_id_internal;

 UPDATE $schema$.streams
 SET is_deleted = true
 WHERE $schema$.streams.id_internal = _stream_id_internal;
 
RETURN;
END;
$BODY$
LANGUAGE plpgsql;