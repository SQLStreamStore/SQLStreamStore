START TRANSACTION;

INSERT INTO Streams (
            Id,
            IdOriginal,
            Version,
            Position)
     SELECT ?streamId,
            ?streamIdOriginal,
            -1, 
            0
       FROM DUAL
      WHERE NOT EXISTS (
     SELECT 1
       FROM Streams
      WHERE Streams.Id = ?streamId
       LOCK IN SHARE MODE);

     SELECT Streams.IdInternal
       FROM Streams
      WHERE Streams.Id = ?streamId
       INTO @streamIdInternal;

    (SELECT Messages.StreamVersion,
            Messages.Position,
            '' 
       FROM Messages
      WHERE Messages.StreamIdInternal = @streamIdInternal
   ORDER BY Messages.Position DESC
      LIMIT 1)

      UNION
    
    (SELECT -1,
             0,
             ''
       FROM DUAL
      WHERE NOT EXISTS (
     SELECT 1
       FROM Messages
      WHERE Messages.StreamIdInternal = @streamIdInternal
       LOCK IN SHARE MODE));
      
COMMIT;