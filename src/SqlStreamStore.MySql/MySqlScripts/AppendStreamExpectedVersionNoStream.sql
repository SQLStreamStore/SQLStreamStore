START TRANSACTION;

INSERT INTO Streams (
            Id,
            IdOriginal)
     VALUES (
            ?streamId,
            ?streamIdOriginal);

     SELECT LAST_INSERT_ID(), -1
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
       INTO @latestStreamVersion,
            @latestStreamPosition
       FROM Messages
      WHERE Messages.StreamIdInternal = @streamIdInternal
   ORDER BY Messages.Position DESC
      LIMIT 1;

     UPDATE Streams
        SET Streams.Version = @latestStreamVersion,
            Streams.Position = @latestStreamPosition
      WHERE Streams.IdInternal = @streamIdInternal;

     SELECT Streams.IdInternal
       INTO @metadataStreamIdInternal
       FROM Streams
      WHERE Streams.Id = CONCAT('$$', ?streamId);
COMMIT;

/* Select CurrentVersion, CurrentPosition */
    (SELECT @latestStreamVersion, @latestStreamPosition, '')

      UNION

    (SELECT -1, -1, Messages.JsonData
       FROM Messages
      WHERE Messages.StreamIdInternal = @metadataStreamIdInternal
   ORDER BY Messages.Position DESC
      LIMIT 1);
