DROP TABLE IF EXISTS events;
DROP TABLE IF EXISTS streams;
DROP TYPE IF EXISTS new_stream_events;
CREATE TABLE streams(
    id text NOT NULL,
    id_original text NOT NULL,
    id_internal SERIAL PRIMARY KEY NOT NULL,
    is_deleted boolean DEFAULT (false) NOT NULL
);
CREATE UNIQUE INDEX ix_streams_id ON streams USING btree(id);

CREATE TABLE events(
    stream_id_internal integer NOT NULL,
    stream_version integer NOT NULL,
    ordinal SERIAL PRIMARY KEY NOT NULL ,
    id uuid NOT NULL,
    created timestamp NOT NULL,
    type text NOT NULL,
    json_data json NOT NULL,
    json_metadata json ,
    CONSTRAINT fk_events_streams FOREIGN KEY (stream_id_internal) REFERENCES streams(id_internal)
);

CREATE UNIQUE INDEX ix_events_stream_id_internal_revision ON events USING btree(stream_id_internal, stream_version);

CREATE TYPE new_stream_events AS (
    stream_version integer,
    id uuid,
    created timestamp,
    type text,
    json_data json,
    json_metadata json
);
 
-- ExpectedVersion.NoStream

new_events new_stream_events;
stream_id text;

stream_id = 'stream-1';

 
INSERT INTO @newEvents
    (
        Id              ,
        [Type]          ,
        JsonData        ,
        JsonMetadata
    ) VALUES
    ('00000000-0000-0000-0000-000000000001', 'type1', '\"data1\"', '\"meta1\"'),
    ('00000000-0000-0000-0000-000000000002', 'type2', '\"data2\"', '\"meta2\"'),
    ('00000000-0000-0000-0000-000000000003', 'type3', '\"data3\"', '\"meta3\"'),
    ('00000000-0000-0000-0000-000000000004', 'type4', '\"data4\"', '\"meta4\"');
 
-- Actual SQL statement of interest
SET TRANSACTION ISOLATION LEVEL SERIALIZABLE;
BEGIN TRANSACTION CreateStream;
    DECLARE @count AS INT;
    DECLARE @streamIdInternal AS INT;
    BEGIN
        INSERT INTO dbo.Streams (Id, IdOriginal) VALUES (@streamId, @streamId);
        SELECT @streamIdInternal = SCOPE_IDENTITY();

        INSERT INTO dbo.Events (StreamIdInternal, StreamVersion, Id, Created, [Type], JsonData, JsonMetadata)
             SELECT @streamIdInternal,
                    StreamVersion,
                    Id,
                    Created,
                    [Type],
                    JsonData,
                    JsonMetadata
               FROM @newEvents;
 
    END;
    SELECT @streamIdInternal;
COMMIT TRANSACTION CreateStream;


SET @streamId = 'stream-2';
SET TRANSACTION ISOLATION LEVEL SERIALIZABLE;
BEGIN TRANSACTION CreateStream;
    BEGIN
        INSERT INTO dbo.Streams (Id, IdOriginal) VALUES (@streamId, @streamId);
        SELECT @streamIdInternal = SCOPE_IDENTITY();

        INSERT INTO dbo.Events (StreamIdInternal, StreamVersion, Id, Created, [Type], JsonData, JsonMetadata)
             SELECT @streamIdInternal,
                    StreamVersion,
                    Id,
                    Created,
                    [Type],
                    JsonData,
                    JsonMetadata
               FROM @newEvents
 
    END;
    SELECT @streamIdInternal;
COMMIT TRANSACTION CreateStream;
 
SELECT * FROM dbo.Streams;
SELECT * FROM dbo.Events;

DECLARE @pageNumber AS INT, @rowspPage AS INT;
SET @pageNumber = 2;
SET @rowspPage = 5;

/* SQL Server 2012+ */
     SELECT Streams.IdOriginal As StreamId,
            Events.StreamVersion,
            Events.Ordinal,
            Events.Id AS EventId,
            Events.Created,
            Events.Type,
            Events.JsonData,
            Events.JsonMetadata
       FROM Events
 INNER JOIN Streams
         ON Events.StreamIdInternal=Streams.IdInternal
   ORDER BY Events.Ordinal
     OFFSET ((@pageNumber - 1) * @rowspPage) ROWS
 FETCH NEXT @rowspPage ROWS ONLY;

 /* SQL Server 2000+ */
     SELECT Id As StreamId,
            StreamVersion,
            Ordinal,
            EventId,
            Created,
            [Type],
            JsonData,
            JsonMetadata
       FROM (
             SELECT ROW_NUMBER() OVER(ORDER BY Events.Ordinal) AS NUMBER,
                    Events.StreamIdInternal,
                    Events.StreamVersion,
                    Events.Ordinal,
                    Events.Id AS EventId,
                    Events.Created,
                    Events.Type,
                    Events.JsonData,
                    Events.JsonMetadata
               FROM Events
               ) AS PageTable
 INNER JOIN Streams
         ON StreamIdInternal=Streams.IdInternal
      WHERE NUMBER BETWEEN ((@pageNumber - 1) * @RowspPage + 1) AND (@pageNumber * @rowspPage)
   ORDER BY NUMBER;

DECLARE @ordinal AS INT, @count1 AS INT;
SET @ordinal = 2;
SET @count1 = 5

/* SQL Server 2008+ */
     SELECT TOP(@count1)
            Streams.IdOriginal As StreamId,
            Events.StreamVersion,
            Events.Ordinal,
            Events.Id AS EventId,
            Events.Created,
            Events.Type,
            Events.JsonData,
            Events.JsonMetadata
       FROM Events
 INNER JOIN Streams
         ON Events.StreamIdInternal=Streams.IdInternal
      WHERE Events.Ordinal >= @ordinal
   ORDER BY Events.Ordinal;

   /* SQL Server 2008+ */
     SELECT TOP(@count1)
            Streams.IdOriginal As StreamId,
            Events.StreamVersion,
            Events.Ordinal,
            Events.Id AS EventId,
            Events.Created,
            Events.Type,
            Events.JsonData,
            Events.JsonMetadata
       FROM Events
 INNER JOIN Streams
         ON Events.StreamIdInternal=Streams.IdInternal
      WHERE Events.Ordinal <= @ordinal
   ORDER BY Events.Ordinal DESC;

/* Delete Streeam*/
BEGIN TRANSACTION DeleteStream
         SELECT @streamIdInternal = Streams.IdInternal
           FROM Streams
          WHERE Streams.Id = @streamId;

    DELETE FROM Events
          WHERE Events.StreamIdInternal = @streamIdInternal;
       
         UPDATE Streams
            SET IsDeleted = '1'
          WHERE Streams.Id = @streamId;
COMMIT TRANSACTION DeleteStream

SELECT * FROM dbo.Streams;
SELECT * FROM dbo.Events;

/* ReadStreamForward */

SET @count = 5;
SET @streamId = 'stream-1';
DECLARE @StreamVersion AS INT = 0
DECLARE @isDeleted AS BIT;

     SELECT @streamIdInternal = Streams.IdInternal,
            @isDeleted = Streams.IsDeleted
       FROM Streams
      WHERE Streams.Id = @streamId

     SELECT @isDeleted AS IsDeleted

     SELECT TOP(@count)
            Events.StreamVersion,
            Events.Ordinal,
            Events.Id AS EventId,
            Events.Created,
            Events.Type,
            Events.JsonData,
            Events.JsonMetadata
       FROM Events
      INNER JOIN Streams
         ON Events.StreamIdInternal = Streams.IdInternal
      WHERE Events.StreamIDInternal = @streamIDInternal AND Events.StreamVersion >= @StreamVersion
   ORDER BY Events.Ordinal;

     SELECT TOP(1)
            Events.StreamVersion
       FROM Events
      WHERE Events.StreamIDInternal = @streamIDInternal
   ORDER BY Events.Ordinal DESC;

/* ReadStreamBackward */

SET @StreamVersion = 5;

     SELECT @streamIdInternal = Streams.IdInternal,
            @isDeleted = Streams.IsDeleted
       FROM Streams
      WHERE Streams.Id = @streamId

     SELECT @isDeleted;

     SELECT TOP(@count)
            Streams.IdOriginal As StreamId,
            Streams.IsDeleted as IsDeleted,
            Events.StreamVersion,
            Events.Ordinal,
            Events.Id AS EventId,
            Events.Created,
            Events.Type,
            Events.JsonData,
            Events.JsonMetadata
       FROM Events
 INNER JOIN Streams
         ON Events.StreamIdInternal = Streams.IdInternal
      WHERE Events.StreamIDInternal = @streamIDInternal AND Events.StreamVersion <= @StreamVersion
   ORDER BY Events.Ordinal DESC

     SELECT TOP(1)
            Events.StreamVersion
       FROM Events
      WHERE Events.StreamIDInternal = @streamIDInternal
   ORDER BY Events.Ordinal DESC;

/* Delete Stream with expected version */ 
SET TRANSACTION ISOLATION LEVEL SERIALIZABLE;
BEGIN TRANSACTION DeleteStream
        DECLARE @streamIdInternal2 AS INT;
        DECLARE @expectedStreamVersion AS INT = 3;
        DECLARE @latestStreamVersion  AS INT;
        SET @streamId = 'stream-1';

         SELECT @streamIdInternal2 = Streams.IdInternal
           FROM Streams
          WHERE Streams.Id = @streamId;

          IF @streamIdInternal2 IS NULL
          BEGIN
             ROLLBACK TRANSACTION DeleteStream;
             RAISERROR('WrongExpectedVersion', 12,1);
          END

          SELECT TOP(1)
                @latestStreamVersion = Events.StreamVersion
           FROM Events
          WHERE Events.StreamIDInternal = @streamIdInternal2
       ORDER BY Events.Ordinal DESC;

         IF @latestStreamVersion != @expectedStreamVersion
         BEGIN
            ROLLBACK TRANSACTION DeleteStream;
            RAISERROR('WrongExpectedVersion', 12,2);
         END

         UPDATE Streams
            SET IsDeleted = '1'
          WHERE Streams.Id = @streamId ;

         DELETE FROM Events
          WHERE Events.StreamIdInternal = @streamIdInternal2;

COMMIT TRANSACTION DeleteStream