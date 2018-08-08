DROP FUNCTION IF EXISTS __schema__.append_to_stream(
  CHAR(42),
  VARCHAR(1000),
  INT,
  TIMESTAMP
) CASCADE;
DROP FUNCTION IF EXISTS __schema__.delete_stream(
  CHAR(42),
  INT,
  TIMESTAMP,
  CHAR(42),
  VARCHAR(1000),
  __schema__ .new_stream_message
) CASCADE;
DROP FUNCTION IF EXISTS __schema__.delete_stream_messages(
  CHAR(42),
    UUID [],
  CHAR(42),
  VARCHAR(1000),
  TIMESTAMP,
  __schema__ .new_stream_message []
) CASCADE;
DROP FUNCTION IF EXISTS __schema__.enforce_idempotent_append(
  CHAR(42),
  INT,
  BOOLEAN,
  __schema__ .new_stream_message []
) CASCADE;
DROP FUNCTION IF EXISTS __schema__.read(
  CHAR(42),
  INT,
  INT,
  BOOLEAN,
  BOOLEAN
) CASCADE;
DROP FUNCTION IF EXISTS __schema__.read_all(
  INT,
  BIGINT,
  BOOLEAN,
  BOOLEAN
) CASCADE;
DROP FUNCTION IF EXISTS __schema__.read_schema_version() CASCADE;
DROP FUNCTION IF EXISTS __schema__.read_head_position() CASCADE;
DROP FUNCTION IF EXISTS __schema__.read_json_data(
  CHAR(42),
  INT
) CASCADE;
DROP FUNCTION IF EXISTS __schema__.read_stream_message_before_created_count(
  CHAR(42),
  TIMESTAMP
) CASCADE;
DROP FUNCTION IF EXISTS __schema__.read_stream_version_of_message_id(
  INT,
  UUID
);
DROP FUNCTION IF EXISTS __schema__.scavenge(
  CHAR(42),
  INT
) CASCADE;
DROP TABLE IF EXISTS __schema__.deleted_streams CASCADE;
DROP TABLE IF EXISTS __schema__.messages CASCADE;
DROP SEQUENCE IF EXISTS __schema__.messages_seq CASCADE;
DROP TABLE IF EXISTS __schema__.streams CASCADE;
DROP SEQUENCE IF EXISTS __schema__.streams_seq CASCADE;
DROP TYPE IF EXISTS __schema__.new_stream_message CASCADE;