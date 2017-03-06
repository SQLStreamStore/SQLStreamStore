BEGIN TRANSACTION CreateStream;
    DECLARE @streamIdInternal AS INT;
    DECLARE @latestStreamVersion AS INT;
	DECLARE @latestStreamPosition AS BIGINT;

    BEGIN
        INSERT INTO dbo.Streams (Id, IdOriginal) VALUES (@streamId, @streamIdOriginal);
        SELECT @streamIdInternal = SCOPE_IDENTITY();

            INSERT INTO dbo.Messages (StreamIdInternal, StreamVersion, Id, Created, [Type], JsonData, JsonMetadata)
                 SELECT @streamIdInternal,
                        StreamVersion,
                        Id,
                        Created,
                        [Type],
                        JsonData,
                        JsonMetadata
                   FROM @newMessages
               ORDER BY StreamVersion;

             SELECT TOP(1)
                    @latestStreamVersion = dbo.Messages.StreamVersion,
					@latestStreamPosition = dbo.Messages.Position
              FROM dbo.Messages
             WHERE dbo.Messages.StreamIDInternal = @streamIdInternal
          ORDER BY dbo.Messages.Position DESC

             IF @latestStreamVersion IS NULL
            SET @latestStreamVersion = -1

			 IF @latestStreamPosition IS NULL
            SET @latestStreamPosition = -1

            UPDATE dbo.Streams
               SET dbo.Streams.[Version] = @latestStreamVersion,
				   dbo.Streams.[Position] = @latestStreamPosition
             WHERE dbo.Streams.IdInternal = @streamIdInternal
    END;
COMMIT TRANSACTION CreateStream;

/* Select CurrentVersion, CurrentPosition */

     SELECT currentVersion = @latestStreamVersion, currentPosition = @latestStreamPosition

/* Select Metadata */
    DECLARE @metadataStreamId as NVARCHAR(42)
    DECLARE @metadataStreamIdInternal as INT
        SET @metadataStreamId = '$$' + @streamId

     SELECT @metadataStreamIdInternal = dbo.Streams.IdInternal
       FROM dbo.Streams
      WHERE dbo.Streams.Id = @metadataStreamId;

     SELECT TOP(1)
            dbo.Messages.JsonData
       FROM dbo.Messages
      WHERE dbo.Messages.StreamIdInternal = @metadataStreamIdInternal
   ORDER BY dbo.Messages.Position DESC;
