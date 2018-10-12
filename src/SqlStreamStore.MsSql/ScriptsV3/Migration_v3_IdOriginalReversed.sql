UPDATE [dbo].[Streams]
SET [IdOriginalReversed] = REVERSE([IdOriginal]);

ALTER TABLE [dbo].[Streams]
    ALTER COLUMN [IdOriginalReversed] NVARCHAR(1000) NOT NULL