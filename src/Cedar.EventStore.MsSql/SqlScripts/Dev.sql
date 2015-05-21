DROP TABLE dbo.Events;
DROP TABLE dbo.Streams;
DROP TYPE dbo.NewStreamEvents;

CREATE TABLE dbo.Streams(
    Id                  CHAR(40)                                NOT NULL,
    IdOriginal          NVARCHAR(1000)                          NOT NULL,
    IdInternal          INT                 IDENTITY(1,1)       NOT NULL,
    IsDeleted           BIT                 DEFAULT (0)         NOT NULL,
    CONSTRAINT PK_Streams PRIMARY KEY CLUSTERED (IdInternal)
);
CREATE UNIQUE NONCLUSTERED INDEX IX_Streams_Id ON dbo.Streams (Id);
 
CREATE TABLE dbo.Events(
    StreamIdInternal    INT                                     NOT NULL,
    StreamRevision      INT                                     NOT NULL,
    Ordinal             BIGINT                 IDENTITY(0,1)    NOT NULL,
    Id                  UNIQUEIDENTIFIER                        NOT NULL,
    Created             DATETIME                                NOT NULL,
    [Type]              NVARCHAR(128)                           NOT NULL,
    JsonData            NVARCHAR(max)                           NOT NULL,
    JsonMetadata        NVARCHAR(max)                                   ,
    CONSTRAINT PK_Events PRIMARY KEY CLUSTERED (Ordinal),
    CONSTRAINT FK_Events_Streams FOREIGN KEY (StreamIdInternal) REFERENCES dbo.Streams(IdInternal)
);

CREATE UNIQUE NONCLUSTERED INDEX IX_Events_StreamIdInternal_Revision ON dbo.Events (StreamIdInternal, StreamRevision);

CREATE TYPE dbo.NewStreamEvents AS TABLE (
    StreamRevision      INT IDENTITY(0,1)                       NOT NULL,
    Id                  UNIQUEIDENTIFIER                        NOT NULL,
    Created             DATETIME            DEFAULT(GETDATE())  NOT NULL,
    [Type]              NVARCHAR(128)                           NOT NULL,
    JsonData            NVARCHAR(max)                           NULL    ,
    JsonMetadata        NVARCHAR(max)                           NULL
);
GO
 
-- ExpectedVersion.NoStream

DECLARE @newEvents dbo.NewStreamEvents;

DECLARE @streamId CHAR(40) = 'stream-1';
 
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

        INSERT INTO dbo.Events (StreamIdInternal, StreamRevision, Id, Created, [Type], JsonData, JsonMetadata)
             SELECT @streamIdInternal,
                    StreamRevision,
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

        INSERT INTO dbo.Events (StreamIdInternal, StreamRevision, Id, Created, [Type], JsonData, JsonMetadata)
             SELECT @streamIdInternal,
                    StreamRevision,
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
            Events.StreamRevision,
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
            StreamRevision,
            Ordinal,
            EventId,
            Created,
            [Type],
            JsonData,
            JsonMetadata
       FROM (
             SELECT ROW_NUMBER() OVER(ORDER BY Events.Ordinal) AS NUMBER,
                    Events.StreamIdInternal,
                    Events.StreamRevision,
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
            Events.StreamRevision,
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
            Events.StreamRevision,
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
DECLARE @streamRevision AS INT = 0
DECLARE @isDeleted AS BIT;

     SELECT @streamIdInternal = Streams.IdInternal,
            @isDeleted = Streams.IsDeleted
       FROM Streams
      WHERE Streams.Id = @streamId

     SELECT @isDeleted AS IsDeleted

     SELECT TOP(@count)
            Events.StreamRevision,
            Events.Ordinal,
            Events.Id AS EventId,
            Events.Created,
            Events.Type,
            Events.JsonData,
            Events.JsonMetadata
       FROM Events
      INNER JOIN Streams
         ON Events.StreamIdInternal = Streams.IdInternal
      WHERE Events.StreamIDInternal = @streamIDInternal AND Events.StreamRevision >= @streamRevision
   ORDER BY Events.Ordinal;

     SELECT TOP(1)
            Events.StreamRevision
       FROM Events
      WHERE Events.StreamIDInternal = @streamIDInternal
   ORDER BY Events.Ordinal DESC;

/* ReadStreamBackward */

SET @streamRevision = 5;

     SELECT @streamIdInternal = Streams.IdInternal,
            @isDeleted = Streams.IsDeleted
       FROM Streams
      WHERE Streams.Id = @streamId

     SELECT @isDeleted;

     SELECT TOP(@count)
            Streams.IdOriginal As StreamId,
            Streams.IsDeleted as IsDeleted,
            Events.StreamRevision,
            Events.Ordinal,
            Events.Id AS EventId,
            Events.Created,
            Events.Type,
            Events.JsonData,
            Events.JsonMetadata
       FROM Events
 INNER JOIN Streams
         ON Events.StreamIdInternal = Streams.IdInternal
      WHERE Events.StreamIDInternal = @streamIDInternal AND Events.StreamRevision <= @streamRevision
   ORDER BY Events.Ordinal DESC

     SELECT TOP(1)
            Events.StreamRevision
       FROM Events
      WHERE Events.StreamIDInternal = @streamIDInternal
   ORDER BY Events.Ordinal DESC;