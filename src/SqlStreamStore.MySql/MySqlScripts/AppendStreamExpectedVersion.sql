START TRANSACTION;
     SELECT Streams.IdInternal,
            Streams.Version
       FROM Streams
      WHERE Streams.Id = ?streamId
       INTO @streamIdInternal,
            @latestStreamVersion;

INSERT INTO Messages (
            StreamIdInternal,
            StreamVersion,
            Id,
            Created,
            Type,
            JsonData,
            JsonMetadata)
     VALUES
{0}
;

     SELECT Messages.StreamVersion,
            Messages.Position
       FROM Messages
      WHERE Messages.StreamIdInternal = @streamIdInternal
   ORDER BY Messages.Position DESC
      LIMIT 1
       INTO @latestStreamVersion,
            @latestStreamPosition;

     UPDATE Streams
        SET Streams.Version = @latestStreamVersion,
            Streams.Position = @latestStreamPosition
      WHERE Streams.IdInternal = @streamIdInternal;

     SELECT Streams.IdInternal
       FROM Streams
      WHERE Streams.Id = CONCAT('$$', ?streamId)
       INTO @metadataStreamIdInternal;
COMMIT;

/* Select CurrentVersion, CurrentPosition */
    (SELECT @latestStreamVersion,
            @latestStreamPosition,
            '')

      UNION

    (SELECT -1,
            -1,
            Messages.JsonData
       FROM Messages
      WHERE Messages.StreamIdInternal = @metadataStreamIdInternal
   ORDER BY Messages.Position DESC
      LIMIT 1);
