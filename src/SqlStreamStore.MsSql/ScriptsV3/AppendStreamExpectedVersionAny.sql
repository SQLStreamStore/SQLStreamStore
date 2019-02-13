DECLARE @streamIdInternal AS INT;
DECLARE @maxCount as INT;

BEGIN TRANSACTION CreateStreamIfNotExists;

    IF NOT EXISTS (
        SELECT *
        FROM dbo.Streams WITH (NOLOCK)
        WHERE dbo.Streams.Id = @streamId
    )
        BEGIN
            BEGIN TRY
                INSERT INTO dbo.Streams (Id, IdOriginal)
                VALUES (@streamId, @streamIdOriginal);
            END TRY
            BEGIN CATCH
                IF ERROR_NUMBER() NOT IN (2601, 2627) THROW;	-- rethrow errors not caued by unique violations	
            END CATCH

            -- If metadata exists, lift maxAge and maxCount. TODO put this into a function? Duplicate code in Append...NoStream.sql
            DECLARE @jsonData nvarchar(max);
            DECLARE @startIndex int;
            DECLARE @endIndex int;
            DECLARE @metadataMaxAgeString nvarchar(50);
            DECLARE @metadataMaxAge int;
            DECLARE @metadataMaxCountString nvarchar(50);
            DECLARE @metadataMaxCount int;

            IF EXISTS(SELECT 1 FROM dbo.Streams WITH(NOLOCK)
                    WHERE dbo.Streams.Id = '$$' + @streamId)
                BEGIN
                -- ...read metadata backwards by 1
                    SELECT TOP(1)
                        @jsonData = dbo.Messages.JsonData
                    FROM dbo.Messages
                INNER JOIN dbo.Streams
                        ON dbo.Messages.StreamIdInternal = dbo.Streams.IdInternal
                    WHERE dbo.Streams.Id = '$$' + @streamId
                ORDER BY dbo.Messages.Position DESC;

                -- Extract MaxAge and MaxCount from Json...
                    SELECT @startIndex = CHARINDEX('"MaxAge":', @jsonData) + 9;
                    SELECT @endIndex = CHARINDEX(',', @jsonData, @startIndex);
                    SELECT @metadataMaxAgeString = SUBSTRING(@jsonData, @startIndex, @endIndex - @startIndex);
                    SELECT @metadataMaxAge = CASE 
                                WHEN @metadataMaxAgeString = 'null' 
                                    THEN NULL 
                                    ELSE CAST(@metadataMaxAgeString as INT)
                                END;

                    SELECT @startIndex = CHARINDEX('"MaxCount":', @jsonData) + 11;
                    SELECT @endIndex = CHARINDEX(',', @jsonData, @startIndex);
                    SELECT @metadataMaxCountString = SUBSTRING(@jsonData,@startIndex, @endIndex - @startIndex);
                    SELECT @metadataMaxCount = CASE 
                                WHEN @metadataMaxCountString = 'null' 
                                    THEN NULL 
                                    ELSE CAST(@metadataMaxCountString as INT)
                                END;

                -- and update the stream row
                    UPDATE dbo.Streams
                    SET dbo.Streams.[MaxAge] = @metadataMaxAge,
                        dbo.Streams.[MaxCount] = @metadataMaxCount
                    WHERE dbo.Streams.[Id] = @streamId
               
              END
        END

COMMIT TRANSACTION CreateStreamIfNotExists;

BEGIN TRANSACTION AppendStream;

    DECLARE @latestStreamVersion AS INT;
    DECLARE @latestStreamPosition AS BIGINT;

    SELECT @streamIdInternal = dbo.Streams.IdInternal,
           @latestStreamVersion = dbo.Streams.[Version],
           @latestStreamPosition = dbo.Streams.[Position]
    FROM dbo.Streams WITH (UPDLOCK, ROWLOCK)
    WHERE dbo.Streams.Id = @streamId;

    IF @hasMessages = 1
        BEGIN
            INSERT INTO dbo.Messages
                (StreamIdInternal, StreamVersion, Id, Created, [Type], JsonData, JsonMetadata)
                SELECT @streamIdInternal, StreamVersion + @latestStreamVersion + 1, Id, Created, [Type], JsonData, JsonMetadata
                FROM @newMessages
                ORDER BY StreamVersion

            SET @latestStreamPosition = SCOPE_IDENTITY()

            SELECT @latestStreamVersion = MAX(StreamVersion) + @latestStreamVersion + 1
            FROM @newMessages
            SET @latestStreamVersion = @latestStreamVersion

            UPDATE dbo.Streams
                SET dbo.Streams.[Version] = @latestStreamVersion,
                    dbo.Streams.[Position] = @latestStreamPosition
                WHERE dbo.Streams.IdInternal = @streamIdInternal
        END
    ELSE
        BEGIN
            SET @latestStreamPosition = ISNULL(@latestStreamPosition, -1)
            SET @latestStreamVersion = ISNULL(@latestStreamVersion, -1)
        END

COMMIT TRANSACTION AppendStream;

   SELECT currentVersion = @latestStreamVersion,
          currentPosition = @latestStreamPosition,
          maxCount = dbo.Streams.MaxCount
     FROM dbo.Streams
    WHERE dbo.Streams.IdInternal = @streamIdInternal;