DROP TABLE dbo.Events
DROP TABLE dbo.Streams
DROP TYPE dbo.NewStreamEvents

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
    Ordinal             INT                 IDENTITY(0,1)       NOT NULL,
    Id                  UNIQUEIDENTIFIER                        NOT NULL,
    Created             DATETIME                                NOT NULL,
    [Type]              NVARCHAR(128)                           NOT NULL,
    JsonData            NVARCHAR(max)                           NOT NULL,
    JsonMetadata        NVARCHAR(max)                                  ,
    CONSTRAINT PK_Events PRIMARY KEY CLUSTERED (Ordinal),
    CONSTRAINT FK_Events_Streams FOREIGN KEY (StreamIdInternal) REFERENCES dbo.Streams(IdInternal)
);

CREATE UNIQUE NONCLUSTERED INDEX IX_Events_StreamIdInternal_Revision ON dbo.Events (StreamIdInternal, StreamRevision);

CREATE TYPE dbo.NewStreamEvents AS TABLE (
    StreamRevision      INT IDENTITY(0,1)                       NOT NULL,
    Id                  UNIQUEIDENTIFIER    DEFAULT(NEWID())    NULL    ,
    Created             DATETIME            DEFAULT(GETDATE())  NULL    ,
    [Type]              NVARCHAR(128)                           NOT NULL,
    JsonData            NVARCHAR(max)                           NULL    ,
    JsonMetadata        NVARCHAR(max)                           NULL
)
GO
 
-- ExpectedVersion.NoStream

DECLARE @newEvents dbo.NewStreamEvents

DECLARE @streamId CHAR(40) = 'stream-1';
 
INSERT INTO @newEvents
    (
        [Type]          ,
        JsonData        ,
        JsonMetadata
    ) VALUES
    ('type1',    '\"data1\"',    '\"meta1\"'),
    ('type2',    '\"data2\"',    '\"meta2\"'),
    ('type3',    '\"data3\"',    '\"meta3\"'),
    ('type4',    '\"data4\"',    '\"meta4\"')
 
-- Actual SQL statement of interest
SET TRANSACTION ISOLATION LEVEL SERIALIZABLE;
BEGIN TRANSACTION CreateStream
    DECLARE @count AS INT;
    DECLARE @streamIdInternal AS INT;
    BEGIN
        INSERT INTO dbo.Streams (Id, IdOriginal) VALUES (@streamId, @streamId);
        SELECT @streamIdInternal = SCOPE_IDENTITY();

        INSERT INTO dbo.Events (StreamIdInternal, StreamRevision, Id, Created, [Type], JsonData, JsonMetadata)
            SELECT  @streamIdInternal,
                    StreamRevision,
                    Id,
                    Created,
                    [Type],
                    JsonData,
                    JsonMetadata
                FROM @newEvents
 
    END
    SELECT @streamIdInternal
COMMIT TRANSACTION CreateStream


SET @streamId = 'stream-2'
SET TRANSACTION ISOLATION LEVEL SERIALIZABLE;
BEGIN TRANSACTION CreateStream
    BEGIN
        INSERT INTO dbo.Streams (Id, IdOriginal) VALUES (@streamId, @streamId);
        SELECT @streamIdInternal = SCOPE_IDENTITY();

        INSERT INTO dbo.Events (StreamIdInternal, StreamRevision, Id, Created, [Type], JsonData, JsonMetadata)
            SELECT  @streamIdInternal,
                    StreamRevision,
                    Id,
                    Created,
                    [Type],
                    JsonData,
                    JsonMetadata
                FROM @newEvents
 
    END
    SELECT @streamIdInternal
COMMIT TRANSACTION CreateStream
 
SELECT * FROM dbo.Streams
SELECT * FROM dbo.Events