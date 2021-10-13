CREATE OR REPLACE FUNCTION __schema__.append_to_stream(
  _stream_id           CHAR(42),
  _stream_id_original  VARCHAR(1000),
  _metadata_stream_id  CHAR(42),
  _expected_version    INT,
  _created_utc         TIMESTAMP,
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
                                     json_metadata,
                                     tx_id)
    SELECT m.message_id, _stream_id_internal, _current_version + (row_number()
        over ()) :: int, _created_utc, m.type, m.json_data, m.json_metadata, txid_current()
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