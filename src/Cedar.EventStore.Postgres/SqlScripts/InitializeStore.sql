CREATE TABLE streams(
    id_internal SERIAL PRIMARY KEY,
    id text NOT NULL,
    id_original text NOT NULL,
    is_deleted boolean DEFAULT (false) NOT NULL
);

CREATE UNIQUE INDEX ix_streams_id
ON streams
USING btree(id);

CREATE TABLE events(
    stream_id_internal integer NOT NULL,
    stream_version integer NOT NULL,
    ordinal SERIAL PRIMARY KEY NOT NULL ,
    id uuid NOT NULL,
    created timestamp NOT NULL,
    type text NOT NULL,
    json_data json NOT NULL,
    json_metadata json ,
    CONSTRAINT fk_events_streams FOREIGN KEY (stream_id_internal) REFERENCES streams(id_internal)
);

CREATE UNIQUE INDEX ix_events_stream_id_internal_revision
ON events
USING btree(stream_id_internal, stream_version DESC, ordinal DESC);

CREATE OR REPLACE FUNCTION create_stream(_stream_id text, _stream_id_original text)
RETURNS integer AS
$BODY$
DECLARE
    _result integer;
BEGIN
    INSERT INTO streams(id, id_original)
    VALUES (_stream_id, _stream_id_original)
    RETURNING id_internal
    INTO _result;

    RETURN _result;
END;
$BODY$
LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION get_stream(_stream_id text)
RETURNS TABLE(id_internal integer, is_deleted boolean, stream_version integer) AS
$BODY$
BEGIN
    RETURN QUERY
    SELECT streams.id_internal,
           streams.is_deleted,
           (SELECT max(events.stream_version) from events where events.stream_id_internal = streams.id_internal)
    FROM streams
    WHERE streams.id = _stream_id;
END;
$BODY$
LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION read_all_forward(_ordinal bigint, _count integer)
RETURNS TABLE(stream_id integer, stream_version integer, ordinal bigint, event_id uuid, created timestamp, type text, json_data json, json_metadata json) AS
$BODY$
BEGIN
    RETURN QUERY
    SELECT 
                streams.id_original As stream_id,
                events.stream_version,
                events.ordinal,
                events.id AS event_id,
                events.created,
                events.type,
                events.json_data,
                events.json_metadata
           FROM events
     INNER JOIN streams
             ON events.stream_id_internal = streams.id_internal
          WHERE events.ordinal >= _ordinal
       ORDER BY events.ordinal
    LIMIT _count;
END;
$BODY$
LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION read_all_backward(_ordinal bigint, _count integer)
RETURNS TABLE(stream_id integer, stream_version integer, ordinal bigint, event_id uuid, created timestamp, type text, json_data json, json_metadata json) AS
$BODY$
BEGIN
    RETURN QUERY
    SELECT 
                streams.id_original As stream_id,
                events.stream_version,
                events.ordinal,
                events.id AS event_id,
                events.created,
                events.type,
                events.json_data,
                events.json_metadata
           FROM events
     INNER JOIN streams
             ON events.stream_id_internal = streams.id_internal
          WHERE events.ordinal <= _ordinal
       ORDER BY events.ordinal DESC
    LIMIT _count;
END;
$BODY$
LANGUAGE plpgsql;


CREATE OR REPLACE FUNCTION read_stream_forward(_stream_id text, _count integer, _stream_version integer) RETURNS SETOF refcursor AS
$BODY$
DECLARE 
  ref1 refcursor;
  ref2 refcursor;
  ref3 refcursor;  
  _stream_id_internal integer;
  _is_deleted boolean;
BEGIN
SELECT streams.id_internal, streams.is_deleted
 INTO _stream_id_internal, _is_deleted
 FROM streams
 WHERE streams.id = _stream_id;
 
OPEN ref1 FOR 
 SELECT _stream_id_internal, _is_deleted;
RETURN NEXT ref1;


OPEN ref2 FOR 
     SELECT 
            events.stream_version,
            events.ordinal,
            events.id AS event_id,
            events.created,
            events.type,
            events.json_data,
            events.json_metadata
       FROM events
      INNER JOIN streams
         ON events.stream_id_internal = streams.id_internal
      WHERE events.stream_id_internal = _stream_id_internal
      AND   events.stream_version >= _stream_version
      ORDER BY events.ordinal
      LIMIT _count;
RETURN next ref2;

OPEN ref3 FOR 
 SELECT events.stream_version
       FROM events
      WHERE events.stream_id_internal = _stream_id_internal
   ORDER BY events.ordinal DESC
   LIMIT 1;
RETURN next ref3;

RETURN;
END;
$BODY$
LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION read_stream_backward(_stream_id text, _count integer, _stream_version integer) RETURNS SETOF refcursor AS
$BODY$
DECLARE 
  ref1 refcursor;
  ref2 refcursor;
  ref3 refcursor;  
  _stream_id_internal integer;
  _is_deleted boolean;
BEGIN

SELECT streams.id_internal, streams.is_deleted
 INTO _stream_id_internal, _is_deleted
 FROM streams
 WHERE streams.id = _stream_id;
 
OPEN ref1 FOR 
 SELECT _stream_id_internal, _is_deleted;
RETURN NEXT ref1;


OPEN ref2 FOR 
     SELECT 
            events.stream_version,
            events.ordinal,
            events.id AS event_id,
            events.created,
            events.type,
            events.json_data,
            events.json_metadata
       FROM events
      INNER JOIN streams
         ON events.stream_id_internal = streams.id_internal
      WHERE events.stream_id_internal = _stream_id_internal
      AND   events.stream_version <= _stream_version
      ORDER BY events.ordinal DESC
      LIMIT _count;
RETURN next ref2;

OPEN ref3 FOR 
 SELECT events.stream_version
       FROM events
      WHERE events.stream_id_internal = _stream_id_internal
   ORDER BY events.ordinal DESC
   LIMIT 1;
RETURN next ref3;

RETURN;
END;
$BODY$
LANGUAGE plpgsql;


CREATE OR REPLACE FUNCTION delete_stream_any_version(stream_id text) RETURNS VOID AS
$BODY$
DECLARE 
  _stream_id_internal integer;
BEGIN

 SELECT streams.id_internal
 INTO _stream_id_internal
 FROM streams
 WHERE streams.id = stream_id;

 DELETE FROM events
 WHERE events.stream_id_internal = _stream_id_internal;

 UPDATE streams
 SET is_deleted = true
 WHERE streams.id_internal = _stream_id_internal;
 
RETURN;
END;
$BODY$
LANGUAGE plpgsql;


CREATE OR REPLACE FUNCTION delete_stream_expected_version(stream_id text, expected_version integer) RETURNS VOID AS
$BODY$
DECLARE 
  _stream_id_internal integer;
  _lastest_stream_version integer;
BEGIN

 SELECT streams.id_internal
 INTO _stream_id_internal
 FROM streams
 WHERE streams.id = stream_id;

 IF _stream_id_internal IS NULL THEN
    RAISE EXCEPTION  'WrongExpectedVersion'
    USING HINT = 'The Stream ' || stream_id || ' does not exist.';
 END IF;

 SELECT stream_version
 INTO _lastest_stream_version
 FROM events
 WHERE events.stream_id_internal = _stream_id_internal
 ORDER BY events.ordinal DESC
 LIMIT 1; 

 IF (_lastest_stream_version <> expected_version) THEN
    RAISE EXCEPTION  'WrongExpectedVersion'
    USING HINT = 'The Stream ' || stream_id || 'version was expected to be' || expected_version::text || ' but was version ' || _lastest_stream_version::text || '.' ;
 END IF;

 DELETE FROM events
 WHERE events.stream_id_internal = _stream_id_internal;

 UPDATE streams
 SET is_deleted = true
 WHERE streams.id_internal = _stream_id_internal;
 
RETURN;
END;
$BODY$
LANGUAGE plpgsql;