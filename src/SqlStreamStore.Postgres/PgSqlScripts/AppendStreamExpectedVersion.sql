    SELECT *
      INTO STRICT huh
      FROM public.streams
     WHERE public.streams.id = @stream_id
 EXCEPTION
      WHEN NO_DATA_FOUND THEN RAISE EXCEPTION 'WrongExpectedVersion'