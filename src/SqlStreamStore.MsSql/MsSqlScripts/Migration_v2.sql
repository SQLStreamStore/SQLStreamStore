/*** Make sure you back up your database first! ***/

BEGIN
    ALTER TABLE [dbo].[Streams]
    ADD [Position] bigint DEFAULT 0;
END

BEGIN
    UPDATE [dbo].[Streams]
    SET [dbo].[Streams].[Position] = (SELECT MAX([dbo].[Messages].[Position])
      FROM [dbo].[Messages]
      WHERE [dbo].[Messages].[StreamIdInternal] = [dbo].[Streams].[IdInternal])
END

BEGIN
    EXEC sys.sp_addextendedproperty   
    @name = N'version',
    @value = N'2',
    @level0type = N'SCHEMA', @level0name = 'dbo',
    @level1type = N'TABLE',  @level1name = 'Streams';
END