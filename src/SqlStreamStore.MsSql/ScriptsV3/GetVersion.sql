DECLARE @ver nvarchar(128)
SET @ver = CAST(serverproperty('version') AS nvarchar)
SET @ver = SUBSTRING(@ver, 1, CHARINDEX('.', @ver) - 1)