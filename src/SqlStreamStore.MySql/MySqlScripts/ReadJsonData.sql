DROP PROCEDURE IF EXISTS read_json_data;

CREATE PROCEDURE read_json_data(_stream_id CHAR(42),
                                _version INT)

BEGIN

    SELECT messages.json_data
    FROM messages
             JOIN streams ON messages.stream_id_internal = streams.id_internal
    WHERE messages.stream_version = _version
      AND streams.id = _stream_id
    LIMIT 1;

END;