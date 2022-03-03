SET TimeZone='UTC';
ALTER TABLE __schema__.messages ALTER COLUMN "created_utc" TYPE TIMESTAMP WITH TIME ZONE;
COMMENT ON SCHEMA __schema__ IS '{ "version": 2 }';

CREATE OR REPLACE FUNCTION __schema__.append_to_stream(
  _stream_id           CHAR(42),
  _stream_id_original  VARCHAR(1000),
  _metadata_stream_id  CHAR(42),
  _expected_version    INT,
  _created_utc         TIMESTAMP WITH TIME ZONE,
  _new_stream_messages __schema__.new_stream_message [])
  RETURNS TABLE(
    current_version  INT,
    current_position BIGINT
  ) AS $F$
DECLARE
  _current_version    INT;
  _current_position   BIGINT;
  _stream_id_internal INT;
  _success            INT;
  _max_age            INT;
  _max_count          INT;
BEGIN
  IF _created_utc IS NULL
  THEN
    _created_utc = now() at time zone 'utc';
  END IF;

  IF _expected_version < 0
  THEN
    SELECT __schema__.messages.json_data :: JSON->>'MaxAge', __schema__.messages.json_data :: JSON->>'MaxCount'
        INTO _max_age, _max_count
    FROM __schema__.messages
           JOIN __schema__.streams ON __schema__.messages.stream_id_internal = __schema__.streams.id_internal
    WHERE __schema__.streams.id = _metadata_stream_id
    ORDER BY __schema__.messages.stream_version DESC
    LIMIT 1;

    INSERT INTO __schema__.streams (id, id_original, max_age, max_count)
    SELECT _stream_id, _stream_id_original, _max_age, _max_count
    ON CONFLICT DO NOTHING;
    GET DIAGNOSTICS _success = ROW_COUNT;

  END IF;

  IF _expected_version = -1 /* ExpectedVersion.Empty */
  THEN

    IF _success = 0 AND
       cardinality(_new_stream_messages) > 0 AND
       (SELECT __schema__.streams.version FROM __schema__.streams WHERE __schema__.streams.id_internal = _stream_id_internal) > 0
    THEN

      RAISE EXCEPTION 'WrongExpectedVersion';
    END IF;
  ELSIF _expected_version = -3 /* ExpectedVersion.NoStream */
    THEN
      IF _success = 0 AND cardinality(_new_stream_messages) > 0
      THEN
        PERFORM __schema__.enforce_idempotent_append(
                  _stream_id,
                  0,
                  false,
                  _new_stream_messages);
        SELECT version, position, id_internal
            INTO _current_version, _current_position, _stream_id_internal
        FROM __schema__.streams
        WHERE __schema__.streams.id = _stream_id;

        RETURN QUERY
        SELECT _current_version, _current_position;
        RETURN;
      END IF;
  END IF;

  SELECT (CASE _expected_version
            WHEN -2 /* ExpectedVersion.Any */
                    THEN coalesce(
                           __schema__.read_stream_version_of_message_id(
                             __schema__.streams.id_internal,
                             _new_stream_messages [ 1 ].message_id) - 1,
                           __schema__.streams.version)
            WHEN -3 THEN -1 /* ExpectedVersion.NoStream */
            WHEN -1 THEN -1 /* ExpectedVersion.Empty */
            ELSE _expected_version END), __schema__.streams.position, __schema__.streams.id_internal
      INTO _current_version, _current_position, _stream_id_internal
  FROM __schema__.streams
  WHERE __schema__.streams.id = _stream_id;

  IF (_expected_version >= 0 AND (
                                   SELECT __schema__.streams.version
                                   FROM __schema__.streams
                                   WHERE __schema__.streams.id_internal = _stream_id_internal
                                 ) < _expected_version)
  THEN
    RAISE EXCEPTION 'WrongExpectedVersion';
  END IF;

  IF (_expected_version >= 0 AND _stream_id_internal IS NULL)
  THEN
    RAISE EXCEPTION 'WrongExpectedVersion';
  END IF;

  IF cardinality(_new_stream_messages) > 0
  THEN
    INSERT INTO __schema__.messages (message_id,
                                     stream_id_internal,
                                     stream_version,
                                     created_utc,
                                     type,
                                     json_data,
                                     json_metadata)
    SELECT m.message_id, _stream_id_internal, _current_version + (row_number()
        over ()) :: int, _created_utc, m.type, m.json_data, m.json_metadata
    FROM unnest(_new_stream_messages) m
    ON CONFLICT DO NOTHING;
    GET DIAGNOSTICS _success = ROW_COUNT;

    IF (_success <> cardinality(_new_stream_messages))
    THEN
      IF (_expected_version = -2) /* ExpectedVersion.Any */
      THEN

        PERFORM __schema__.enforce_idempotent_append(
                  _stream_id,
                  _current_version + 1 - _success,
                  false,
                  _new_stream_messages);

      ELSEIF _expected_version = -3 /* ExpectedVersion.NoStream */
        THEN
          RAISE EXCEPTION 'WhyAreYouHere'; /* there is no way to get here? */
      ELSEIF _expected_version = -1 /* ExpectedVersion.Empty */
        THEN
          PERFORM __schema__.enforce_idempotent_append(
                    _stream_id,
                    0,
                    false,
                    _new_stream_messages);
      ELSE
        PERFORM __schema__.enforce_idempotent_append(
                  _stream_id,
                  _expected_version + 1 - _success,
                  true,
                  _new_stream_messages);
        SELECT version, position, id_internal
            INTO _current_version, _current_position, _stream_id_internal
        FROM __schema__.streams
        WHERE __schema__.streams.id = _stream_id;

        RETURN QUERY
        SELECT _current_version, _current_position;
        RETURN;
      END IF;
    END IF;

    SELECT COALESCE(__schema__.messages.position, -1), COALESCE(__schema__.messages.stream_version, -1)
        INTO _current_position, _current_version
    FROM __schema__.messages
    WHERE __schema__.messages.stream_id_internal = _stream_id_internal
    ORDER BY __schema__.messages.position DESC
    LIMIT 1;

    UPDATE __schema__.streams
    SET "version"  = _current_version,
        "position" = _current_position
    WHERE id_internal = _stream_id_internal;

    RETURN QUERY
    SELECT _current_version, _current_position;

  ELSE
    RETURN QUERY
    SELECT -1, -1 :: BIGINT;

  END IF;

END;

$F$
LANGUAGE 'plpgsql';


CREATE OR REPLACE FUNCTION __schema__.delete_stream(
  _stream_id                  CHAR(42),
  _expected_version           INT,
  _created_utc                TIMESTAMP WITH TIME ZONE,
  _deletion_tracking_disabled BOOLEAN,
  _deleted_stream_id          CHAR(42),
  _deleted_stream_id_original VARCHAR(1000),
  _deleted_stream_message     __schema__.new_stream_message)
  RETURNS VOID
AS $F$
DECLARE
  _stream_id_internal    INT;
  _latest_stream_version INT;
  _affected              INT;
BEGIN
  IF _created_utc IS NULL
  THEN
    _created_utc = now() at time zone 'utc';
  END IF;
  SELECT __schema__.streams.id_internal
      INTO _stream_id_internal
  FROM __schema__.streams
  WHERE __schema__.streams.id = _stream_id;

  IF _expected_version = -1 /* ExpectedVersion.NoStream */
  THEN
    RAISE EXCEPTION 'WrongExpectedVersion';
  ELSIF _expected_version >= 0 /* ExpectedVersion */
    THEN
      IF _stream_id_internal IS NULL
      THEN
        RAISE EXCEPTION 'WrongExpectedVersion';
      END IF;

      SELECT __schema__.messages.stream_version
          INTO _latest_stream_version
      FROM __schema__.messages
      WHERE __schema__.messages.stream_id_internal = _stream_id_internal
      ORDER BY __schema__.messages.position DESC
      LIMIT 1;

      IF _latest_stream_version != _expected_version
      THEN
        RAISE EXCEPTION 'WrongExpectedVersion';
      END IF;
  END IF;

  DELETE FROM __schema__.messages WHERE __schema__.messages.stream_id_internal = _stream_id_internal;

  DELETE FROM __schema__.streams WHERE __schema__.streams.id = _stream_id;

  IF (_deletion_tracking_disabled = TRUE)
      THEN
      RETURN;
  END IF;

  GET DIAGNOSTICS _affected = ROW_COUNT;

  IF _affected > 0
  THEN
    PERFORM __schema__.append_to_stream(
              _deleted_stream_id,
              _deleted_stream_id_original,
              NULL,
              -2,
              _created_utc,
              ARRAY [_deleted_stream_message]);

  END IF;
END;

$F$
LANGUAGE 'plpgsql';

CREATE OR REPLACE FUNCTION __schema__.delete_stream_messages(
  _stream_id                  CHAR(42),
  _message_ids                UUID [],
  _deletion_tracking_disabled BOOLEAN,
  _deleted_stream_id          CHAR(42),
  _deleted_stream_id_original VARCHAR(1000),
  _created_utc                TIMESTAMP WITH TIME ZONE,
  _deleted_messages           __schema__.new_stream_message []
)
  RETURNS VOID
AS $F$
DECLARE
  _stream_id_internal INT;
  _deleted_count      NUMERIC;
BEGIN
  IF _created_utc IS NULL THEN
    _created_utc = now() at time zone 'utc';
  END IF;

  SELECT __schema__.streams.id_internal
      INTO _stream_id_internal
  FROM __schema__.streams
  WHERE __schema__.streams.id = _stream_id;

  WITH deleted AS (DELETE FROM __schema__.messages
  WHERE __schema__.messages.stream_id_internal = _stream_id_internal
        AND __schema__.messages.message_id = ANY (_message_ids)
  RETURNING *)
  SELECT count(*)
  FROM deleted
      INTO _deleted_count;

  IF (_deletion_tracking_disabled = FALSE AND _deleted_count > 0) 
  THEN
    PERFORM __schema__.append_to_stream(
              _deleted_stream_id,
              _deleted_stream_id_original,
              NULL,
              -2,
              _created_utc,
              _deleted_messages);
  END IF;
END;

$F$
LANGUAGE 'plpgsql';

DROP FUNCTION public.read_all(int4, int8, bool, bool);
CREATE OR REPLACE FUNCTION __schema__.read_all(
  _count    INT,
  _position BIGINT,
  _forwards BOOLEAN,
  _prefetch BOOLEAN
)
  RETURNS TABLE(
    stream_id      VARCHAR(1000),
    message_id     UUID,
    stream_version INT,
    "position"     BIGINT,
    create_utc     TIMESTAMP WITH TIME ZONE,
    "type"         VARCHAR(128),
    json_metadata  JSONB,
    json_data      JSONB,
    max_age        INT
  )
AS $F$
BEGIN

  RETURN QUERY
  WITH messages AS (
      SELECT __schema__.streams.id_original,
             __schema__.messages.message_id,
             __schema__.messages.stream_version,
             __schema__.messages.position,
             __schema__.messages.created_utc,
             __schema__.messages.type,
             __schema__.messages.json_metadata,
             (CASE _prefetch
                WHEN TRUE THEN __schema__.messages.json_data
                ELSE NULL END),
             __schema__.streams.max_age
      FROM __schema__.messages
             INNER JOIN __schema__.streams ON __schema__.messages.stream_id_internal = __schema__.streams.id_internal
      WHERE (CASE
               WHEN _forwards THEN __schema__.messages.position >= _position
               ELSE __schema__.messages.position <= _position END)
      ORDER BY
          (CASE WHEN _forwards THEN __schema__.messages.position END),
          (CASE WHEN not _forwards THEN __schema__.messages.position END) DESC
      LIMIT _count
  )
  SELECT * FROM messages LIMIT _count;
END;
$F$
LANGUAGE 'plpgsql';

CREATE OR REPLACE FUNCTION __schema__.set_stream_metadata(
  _stream_id                   CHAR(42),
  _metadata_stream_id          CHAR(42),
  _metadata_stream_id_original CHAR(42),
  _max_age                     INT,
  _max_count                   INT,
  _expected_version            INT,
  _created_utc                 TIMESTAMP WITH TIME ZONE,
  _metadata_message            __schema__.new_stream_message)
  RETURNS INT AS $F$
DECLARE
  _current_version INT;
BEGIN
  IF _created_utc IS NULL
  THEN
    _created_utc = now() at time zone 'utc';
  END IF;

  SELECT current_version
  FROM __schema__.append_to_stream(
         _metadata_stream_id,
         _metadata_stream_id_original,
         NULL,
         _expected_version,
         _created_utc,
         ARRAY [_metadata_message]
      )
      INTO _current_version;

  UPDATE __schema__.streams
  SET max_age   = _max_age,
      max_count = _max_count
  WHERE __schema__.streams.id = _stream_id;

  RETURN _current_version;
END;
$F$
LANGUAGE 'plpgsql';