/* SQL Server 2008+*/

DECLARE @DBName sysname;
SET @DBName = (SELECT db_name());
DECLARE @SQL varchar(1000);
SET @SQL = 'ALTER DATABASE ['+@DBName+'] SET ALLOW_SNAPSHOT_ISOLATION ON; ALTER DATABASE ['+@DBName+'] SET READ_COMMITTED_SNAPSHOT ON;'; 
exec(@sql)

IF OBJECT_ID('dbo.Streams', 'U') IS NULL
BEGIN
    CREATE TABLE dbo.Streams(
        Id                  CHAR(42)                                NOT NULL,
        IdOriginal          NVARCHAR(1000)                          NOT NULL,
        IdInternal          INT                 IDENTITY(1,1)       NOT NULL,
        [Version]           INT                 DEFAULT(-1)         NOT NULL,
        Position            BIGINT              DEFAULT(-1)         NOT NULL,
        MaxAge              INT                 DEFAULT(NULL),
        MaxCount            INT                 DEFAULT(NULL),
        CONSTRAINT PK_Streams PRIMARY KEY CLUSTERED (IdInternal)
    );
END

IF NOT EXISTS(
    SELECT * 
    FROM sys.indexes
    WHERE name='IX_Streams_Id' AND object_id = OBJECT_ID('dbo.Streams', 'U'))
BEGIN
    CREATE UNIQUE NONCLUSTERED INDEX IX_Streams_Id ON dbo.Streams (Id);
END
 
IF object_id('dbo.Messages', 'U') IS NULL
BEGIN
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
END

IF NOT EXISTS(
    SELECT * 
    FROM sys.indexes
    WHERE name='IX_Messages_StreamIdInternal_Id' AND object_id = OBJECT_ID('dbo.Messages'))
BEGIN
    CREATE UNIQUE NONCLUSTERED INDEX IX_Messages_StreamIdInternal_Id ON dbo.Messages (StreamIdInternal, Id);
END

IF NOT EXISTS(
    SELECT * 
    FROM sys.indexes
    WHERE name='IX_Messages_StreamIdInternal_Revision' AND object_id = OBJECT_ID('dbo.Messages'))
BEGIN
    CREATE UNIQUE NONCLUSTERED INDEX IX_Messages_StreamIdInternal_Revision ON dbo.Messages (StreamIdInternal, StreamVersion);
END

IF NOT EXISTS(
    SELECT * 
    FROM sys.indexes
    WHERE name='IX_Messages_StreamIdInternal_Created' AND object_id = OBJECT_ID('dbo.Messages'))
BEGIN
    CREATE NONCLUSTERED INDEX IX_Messages_StreamIdInternal_Created ON dbo.Messages (StreamIdInternal, Created);
END

IF NOT EXISTS(
    SELECT * 
    FROM sys.table_types tt JOIN sys.schemas s ON tt.schema_id = s.schema_id
    WHERE s.name + '.' + tt.name='dbo.NewStreamMessages')
BEGIN
    CREATE TYPE dbo.NewStreamMessages AS TABLE (
        StreamVersion       INT IDENTITY(0,1)                       NOT NULL,
        Id                  UNIQUEIDENTIFIER                        NOT NULL,
        Created             DATETIME          DEFAULT(GETUTCDATE()) NOT NULL,
        [Type]              NVARCHAR(128)                           NOT NULL,
        JsonData            NVARCHAR(max)                           NULL,
        JsonMetadata        NVARCHAR(max)                           NULL
    );
END

BEGIN
    IF NOT EXISTS (SELECT NULL FROM SYS.EXTENDED_PROPERTIES WHERE [major_id] = OBJECT_ID('dbo.Streams') AND [name] = N'version' AND [minor_id] = 0)
    EXEC sys.sp_addextendedproperty   
    @name = N'version',
    @value = N'3',
    @level0type = N'SCHEMA', @level0name = 'dbo',
    @level1type = N'TABLE',  @level1name = 'Streams';
END
