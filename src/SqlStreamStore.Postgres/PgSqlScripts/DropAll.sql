DROP FUNCTION IF EXISTS public.append_to_stream(
  CHAR(42),
  VARCHAR(1000),
  INT,
  TIMESTAMP
) CASCADE;
DROP FUNCTION IF EXISTS public.delete_stream(
  CHAR(42),
  INT,
  TIMESTAMP,
  CHAR(42),
  VARCHAR(1000),
public .new_stream_message
) CASCADE;
DROP FUNCTION IF EXISTS public.delete_stream_messages(
  CHAR(42),
    UUID [],
  CHAR(42),
  VARCHAR(1000),
  TIMESTAMP,
public .new_stream_message []
) CASCADE;
DROP FUNCTION IF EXISTS public.enforce_idempotent_append(
  CHAR(42),
  INT,
  BOOLEAN,
public .new_stream_message []
) CASCADE;
DROP FUNCTION IF EXISTS public.read(
  CHAR(42),
  INT,
  INT,
  BOOLEAN,
  BOOLEAN
) CASCADE;
DROP FUNCTION IF EXISTS public.read_all(
  INT,
  BIGINT,
  BOOLEAN,
  BOOLEAN
) CASCADE;
DROP FUNCTION IF EXISTS public.read_head_position() CASCADE;
DROP FUNCTION IF EXISTS public.read_json_data(
  CHAR(42),
  INT
) CASCADE;
DROP FUNCTION IF EXISTS public.read_stream_message_before_created_count(
  CHAR(42),
  TIMESTAMP
) CASCADE;
DROP FUNCTION IF EXISTS public.read_stream_version_of_message_id(
  INT,
  UUID
);
DROP FUNCTION IF EXISTS public.scavenge(
  CHAR(42),
  INT
) CASCADE;
DROP TABLE IF EXISTS public.deleted_streams CASCADE;
DROP TABLE IF EXISTS public.messages CASCADE;
DROP SEQUENCE IF EXISTS public.messages_seq CASCADE;
DROP TABLE IF EXISTS public.streams CASCADE;
DROP SEQUENCE IF EXISTS public.streams_seq CASCADE;
DROP TYPE IF EXISTS public.new_stream_message CASCADE;