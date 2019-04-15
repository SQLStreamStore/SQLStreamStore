DROP PROCEDURE IF EXISTS read_head_position;

CREATE PROCEDURE read_head_position()

BEGIN
  SELECT max(messages.position)
  FROM messages;
END;