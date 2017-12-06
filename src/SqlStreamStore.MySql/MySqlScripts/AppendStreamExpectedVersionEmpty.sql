START TRANSACTION;
     SELECT Streams.IdInternal,
            Streams.Version
       FROM Streams
      WHERE Streams.Id = ?streamId
       INTO @streamIdInternal,
            @latestStreamVersion;

     SELECT Messages.StreamVersion,
            Messages.Position
       FROM Messages
      WHERE Messages.StreamIdInternal = @streamIdInternal
   ORDER BY Messages.Position DESC
      LIMIT 1
       INTO @latestStreamVersion,
            @latestStreamPosition;
COMMIT;

/* Select CurrentVersion, CurrentPosition */
    SELECT @latestStreamVersion,
            @latestStreamPosition,
            ''
