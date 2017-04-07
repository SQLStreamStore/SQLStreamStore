/***
    Make sure you back up your database first! 

    If you are using a different schema for the stream and messages,
    replace 'dbo' with your schema name.
    
***/

IF NOT EXISTS (SELECT 1 FROM SYS.COLUMNS WHERE OBJECT_ID = OBJECT_ID(N'[dbo].[Streams]') AND name = 'Position')
ALTER TABLE [dbo].[Streams]
ADD [Position] bigint NOT NULL DEFAULT -1;

GO

UPDATE [dbo].[Streams]
SET [dbo].[Streams].[Position] = (SELECT ISNULL(MAX([dbo].[Messages].[Position]), -1)
    FROM [dbo].[Messages]
    WHERE [dbo].[Messages].[StreamIdInternal] = [dbo].[Streams].[IdInternal])

GO

IF NOT EXISTS (SELECT NULL FROM SYS.EXTENDED_PROPERTIES WHERE [major_id] = OBJECT_ID('dbo.Streams') AND [name] = N'version' AND [minor_id] = 0)
EXEC sys.sp_addextendedproperty
@name = N'version',
@value = N'2',
@level0type = N'SCHEMA', @level0name = 'dbo',
@level1type = N'TABLE',  @level1name = 'Streams';

GO