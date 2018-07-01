IF OBJECT_ID('dbo.Streams', 'U') IS NULL
BEGIN
    SELECT 'version' as name, '0' as value
    RETURN
END

SELECT name, value
FROM fn_listextendedproperty (NULL, 'schema', 'dbo', 'table', default, NULL, NULL);
