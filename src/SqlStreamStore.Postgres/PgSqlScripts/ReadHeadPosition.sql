CREATE OR REPLACE FUNCTION public.read_head_position()
  RETURNS BIGINT
AS $F$
BEGIN
  RETURN (
    SELECT max(public.messages.position)
    FROM public.messages
  );
END;
$F$
LANGUAGE 'plpgsql';