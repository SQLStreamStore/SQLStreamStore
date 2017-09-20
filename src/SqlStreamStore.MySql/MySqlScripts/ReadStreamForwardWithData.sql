     SELECT Streams.IdInternal,
            Streams.Version,
            Streams.Position
       FROM Streams
      WHERE Streams.Id = ?streamId
       INTO @streamIdInternal,
            @lastStreamVersion,
            @lastStreamPosition;

    (SELECT @lastStreamVersion,
            @lastStreamPosition,
            NULL,
            NULL,
            NULL,
            NULL,
            NULL)
     
      UNION

    (SELECT Messages.StreamVersion,
            Messages.Position,
            Messages.Id AS EventId,
            Messages.Created,
            Messages.Type,
            Messages.JsonMetadata,
            Messages.JsonData
       FROM Messages
 INNER JOIN Streams
         ON Messages.StreamIdInternal = Streams.IdInternal
      WHERE Messages.StreamIdInternal = @streamIdInternal
        AND Messages.StreamVersion >= ?streamVersion
   ORDER BY Messages.StreamVersion
      LIMIT ?count);
