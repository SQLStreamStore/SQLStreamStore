CREATE OR REPLACE FUNCTION public.read_head_position()
  RETURNS BIGINT
AS $F$
BEGIN
  SELECT max(public.messages.position)
  FROM public.messages.position;
END;
$F$
LANGUAGE 'plpgsql';