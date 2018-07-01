/***
    Make sure you back up your database first! 

    This script will migrate an MSSQL Stream store schema version 2 (package
    versions prior to 1.1.2) to version 2 (package version 1.2.0 and later).
    
***/

IF NOT EXISTS (SELECT 1 FROM SYS.COLUMNS WHERE OBJECT_ID = OBJECT_ID(N'[dbo].[Streams]') AND name = 'MaxAge')
ALTER TABLE [dbo].[Streams]
ADD [MaxAge] int NULL DEFAULT NULL;

IF NOT EXISTS (SELECT 1 FROM SYS.COLUMNS WHERE OBJECT_ID = OBJECT_ID(N'[dbo].[Streams]') AND name = 'MaxCount')
ALTER TABLE [dbo].[Streams]
ADD [MaxCount] int NULL DEFAULT NULL;

EXEC sys.sp_updateextendedproperty
@name = N'version',
@value = N'3',
@level0type = N'SCHEMA', @level0name = 'dbo',
@level1type = N'TABLE',  @level1name = 'Streams';