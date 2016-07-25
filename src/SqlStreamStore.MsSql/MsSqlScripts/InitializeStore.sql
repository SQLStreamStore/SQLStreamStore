/* SQL Server 2008+*/

DECLARE @DBName sysname;
SET @DBName = (SELECT db_name());
DECLARE @SQL varchar(1000);
SET @SQL = 'ALTER DATABASE '+@DBName+' SET ALLOW_SNAPSHOT_ISOLATION ON; ALTER DATABASE '+@DBName+' SET READ_COMMITTED_SNAPSHOT ON;'; 
exec(@sql)

CREATE TABLE dbo.Streams(
    Id                  CHAR(42)                                NOT NULL,
    IdOriginal          NVARCHAR(1000)                          NOT NULL,
    IdInternal          INT                 IDENTITY(1,1)       NOT NULL,
    [Version]           INT                 DEFAULT(-1)         NOT NULL,
    CONSTRAINT PK_Streams PRIMARY KEY CLUSTERED (IdInternal)
);
CREATE UNIQUE NONCLUSTERED INDEX IX_Streams_Id ON dbo.Streams (Id);
 
CREATE TABLE dbo.Messages(
    StreamIdInternal    INT                                     NOT NULL,
    StreamVersion       INT                                     NOT NULL,
    Position            BIGINT                 IDENTITY(0,1)    NOT NULL,
    Id                  UNIQUEIDENTIFIER                        NOT NULL,
    Created             DATETIME                                NOT NULL,
    [Type]              NVARCHAR(128)                           NOT NULL,
    JsonData            NVARCHAR(max)                           NOT NULL,
    JsonMetadata        NVARCHAR(max)                                   ,
    CONSTRAINT PK_Events PRIMARY KEY CLUSTERED (Position),
    CONSTRAINT FK_Events_Streams FOREIGN KEY (StreamIdInternal) REFERENCES dbo.Streams(IdInternal)
);

CREATE UNIQUE NONCLUSTERED INDEX IX_Messages_Position ON dbo.Messages (Position);

CREATE UNIQUE NONCLUSTERED INDEX IX_Messages_StreamIdInternal_Id ON dbo.Messages (StreamIdInternal, Id);

CREATE UNIQUE NONCLUSTERED INDEX IX_Messages_StreamIdInternal_Revision ON dbo.Messages (StreamIdInternal, StreamVersion);

CREATE NONCLUSTERED INDEX IX_Messages_StreamIdInternal_Created ON dbo.Messages (StreamIdInternal, Created);

CREATE TYPE dbo.NewStreamMessages AS TABLE (
    StreamVersion       INT IDENTITY(0,1)                       NOT NULL,
    Id                  UNIQUEIDENTIFIER                        NOT NULL,
    Created             DATETIME          DEFAULT(GETUTCDATE()) NOT NULL,
    [Type]              NVARCHAR(128)                           NOT NULL,
    JsonData            NVARCHAR(max)                           NULL,
    JsonMetadata        NVARCHAR(max)                           NULL
);