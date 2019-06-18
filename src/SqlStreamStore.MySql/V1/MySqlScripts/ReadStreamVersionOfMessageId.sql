DROP PROCEDURE IF EXISTS read_stream_version_of_message_id;

CREATE PROCEDURE read_stream_version_of_message_id(_stream_id_internal INT,
                                                   _message_id BINARY(16),
                                                   OUT _stream_version INT)

BEGIN

  SELECT messages.stream_version INTO _stream_version
  FROM messages
  WHERE messages.message_id = _message_id
    AND messages.stream_id_internal = _stream_id_internal;

END;
