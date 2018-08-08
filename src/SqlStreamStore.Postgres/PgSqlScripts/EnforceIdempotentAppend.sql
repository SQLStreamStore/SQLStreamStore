CREATE OR REPLACE FUNCTION __schema__.enforce_idempotent_append(
  _stream_id           CHAR(42),
  _start               INT,
  _check_length        BOOLEAN,
  _new_stream_messages __schema__.new_stream_message [])
  RETURNS VOID AS $F$
DECLARE
  _message_id_record RECORD;
  _message_id_cursor REFCURSOR;
  _message_ids       UUID [] = '{}' :: UUID [];
BEGIN
  _message_id_cursor = (SELECT *
                        FROM __schema__.read(_stream_id, cardinality(_new_stream_messages), _start, true, false)
                        OFFSET 1);

  FETCH FROM _message_id_cursor
  INTO _message_id_record;

  WHILE FOUND LOOP
    _message_ids = array_append(_message_ids, _message_id_record.message_id);

    FETCH FROM _message_id_cursor
    INTO _message_id_record;
  END LOOP;

  IF (_check_length AND cardinality(_new_stream_messages) > cardinality(_message_ids))
  THEN
    RAISE EXCEPTION 'WrongExpectedVersion'
    USING HINT = 'Wrong message count';
  END IF;

  IF _message_ids <> (SELECT ARRAY(SELECT n.message_id FROM unnest(_new_stream_messages) n))
  THEN
    RAISE EXCEPTION 'WrongExpectedVersion';
  END IF;
END;
$F$
LANGUAGE 'plpgsql';