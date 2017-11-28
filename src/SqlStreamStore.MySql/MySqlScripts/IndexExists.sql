     SELECT EXISTS(
     SELECT index_name
       FROM INFORMATION_SCHEMA.STATISTICS
      WHERE table_schema = DATABASE()
        AND table_name   = ?tableName
        AND index_name   = ?indexName)