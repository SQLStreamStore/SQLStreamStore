
namespace SqlStreamStore.MsSqlScripts
{
    using System.Collections.Generic;
    using System.Data;
    using System.Data.SqlClient;
    using System.Dynamic;
    using System.IO;
    using System.Linq;
    using System.Text;
    using YamlDotNet.Serialization;

    public static class SqlConnectionExtensions
    {
        public static IEnumerable<dynamic> Query(this SqlConnection connection, string sql, Dictionary<string, string> filters)
        {

            connection.Open();
            using (var cmd = connection.CreateCommand())
            {
                cmd.CommandText = sql;
                foreach (var kvp in filters)
                {
                    cmd.Parameters.Add(kvp.Key, SqlDbType.Variant).Value = kvp.Value;
                }
                using (var reader = cmd.ExecuteReader())
                {
                    dynamic result = new ExpandoObject();
                    var columnNames = Enumerable.Range(0, reader.FieldCount - 1).Select(i => reader.GetName(i)).ToArray();
                    while (reader.Read())
                    {
                        for (int i = 0; i < reader.FieldCount - 1; i++)
                        {
                            result[columnNames[i]] = reader.GetValue(i);
                        }
                        yield return result;
                    }

                }
            }
        }
    }


    public class DatabaseSchema
    {
        public static DatabaseSchema ReadFromYaml(string schemaName)
        {
            var yaml = GetYamlFile("Schema");
            var deserializer = new Deserializer();
            return deserializer.Deserialize<DatabaseSchema>(new StringReader(yaml));
        }

        private static string GetYamlFile(string name)
        {
            using (var s = typeof(DatabaseSchema)
                .Assembly.GetManifestResourceStream($"SqlStreamStore.MsSqlScripts.${name}.yaml"))
            using (var r = new StreamReader(s))
            {
                return r.ReadToEnd();
            }
        }

        public string ToYaml()
        {
            var serializer = new Serializer();
            var builder = new StringBuilder();
            serializer.Serialize(new StringWriter(builder), this);
            return builder.ToString();
        }


        public static DatabaseSchema ReadFromDatabase(string connectionString, string schemaName)
        {
            DatabaseSchema schema = null;
            using (var connection = new SqlConnection(connectionString))
            {
                schema = GetSchema(connection, schemaName);
            }
            return schema;
        }

        private static DatabaseSchema GetSchema(SqlConnection connection, string schemaName)
        {
            var schema = new DatabaseSchema()
            {
                Tables = GetTableDefinitions(connection, schemaName)
            };
            return schema;
        }

        private static SortedDictionary<string, DatabaseTable> GetTableDefinitions(SqlConnection connection, string schemaName)
        {
            var result = new SortedDictionary<string, DatabaseTable>();
            var sql = @"
    SELECT * 
    FROM INFORMATION_SCHEMA.TABLES 
    WHERE TABLE_SCHEMA = @TABLE_SCHEMA";

            var tables = connection.Query(sql,
                new Dictionary<string, string>() { { "table_schema", schemaName } })
                .ToArray();


            foreach (var dbTable in tables)
            {
                var columns = GetColumns(connection, dbTable, schemaName);
                var foreignKeys = GetForeignKeys(connection, dbTable, schemaName);

                var indexes = GetIndexes(connection, dbTable, schemaName);
                result.Add(dbTable.TABLE_NAME, new DatabaseTable()
                {
                    Columns = columns,
                    Indexes = indexes,
                    ForeignKeys = foreignKeys
                });
            }
            return result;
        }

        private static string[] GetForeignKeys(SqlConnection connection, dynamic dbTable, string schema)
        {
            var sql = @"SELECT  obj.name AS FK_NAME,
    sch.name AS [schema_name],
    tab1.name AS [table],
    col1.name AS [column],
    tab2.name AS [referenced_table],
    col2.name AS [referenced_column]
FROM sys.foreign_key_columns fkc
INNER JOIN sys.objects obj
    ON obj.object_id = fkc.constraint_object_id
INNER JOIN sys.tables tab1
    ON tab1.object_id = fkc.parent_object_id
INNER JOIN sys.schemas sch
    ON tab1.schema_id = sch.schema_id
INNER JOIN sys.columns col1
    ON col1.column_id = parent_column_id AND col1.object_id = tab1.object_id
INNER JOIN sys.tables tab2
    ON tab2.object_id = fkc.referenced_object_id
INNER JOIN sys.columns col2
    ON col2.column_id = referenced_column_id AND col2.object_id = tab2.object_id
where sch.name = @TABLE_SCHEMA and tab1.name = @table_name
";
            return connection.Query(sql, new Dictionary<string, string>()
                {
                    { "TABLE_SCHEMA", schema },
                { "TABLE_NAME", dbTable.TABLE_NAME }
                })
                .GroupBy(fk => fk.FK_NAME)
                .Select(fkColumns =>
                    new DatabaseForeignKey(
                        fkColumns.First().referenced_table,

                        // get all the columns in the foreign key mapping. Typically 1, but 
                        // in some cases a combined foreign key
                        new DatabaseForeignKey.ColumnMapping(
                            fkColumns.Select(c => c.column).ToStringJoined(","),
                            fkColumns.Select(c => c.referenced_column).ToStringJoined()))
                    .ToString())
                .ToArray();


        }

        private static string[] GetIndexes(SqlConnection connection, dynamic dbTable, string schema)
        {
            var sql = @"
SELECT
     IndexName = ind.name,
     ColumnName = col.name,
     IsPrimarykey = ind.is_primary_key,
     IsUnique = ind.is_unique,
     Type = ind.type_desc,
     IsIncluded = ic.is_included_column,
     Ordinal = ic.key_ordinal
FROM 
     sys.indexes ind 
INNER JOIN 
     sys.index_columns ic ON  ind.object_id = ic.object_id and ind.index_id = ic.index_id 
INNER JOIN 
     sys.columns col ON ic.object_id = col.object_id and ic.column_id = col.column_id 
INNER JOIN 
     sys.tables t ON ind.object_id = t.object_id 
INNER JOIN 
     sys.schemas s on t.schema_id = s.schema_id
WHERE 
     t.is_ms_shipped = 0
     and s.name = @TABLE_SCHEMA AND t.name=@table_name
ORDER BY 
     t.name, ind.name, ind.index_id, ic.index_column_id";

            return connection.Query(sql, new Dictionary<string, string>()
                {
                    { "TABLE_SCHEMA", schema}, { "TABLE_NAME", dbTable.TABLE_NAME }
                })
                .GroupBy(x => x.IndexName)

                .Select(x =>
                        new DatabaseIndexDefinition(
                                DatabaseIndexDefinition.GetType((bool?)x.First().IsPrimarykey,
                                (bool?)x.First().IsUnique),
                                x.First().Type == "CLUSTERED",
                                BuildIndexedColumns(x.Where(c => !c.IsIncluded)),
                                BuildIndexedColumns(x.Where(c => c.IsIncluded))).ToString())
                .ToArray();
        }

        private static DatabaseIndexDefinition.IndexColumn[] BuildIndexedColumns(IEnumerable<dynamic> x)
        {
            return x
                .OrderBy(column => column.Ordinal)
                .Select(column => new DatabaseIndexDefinition.IndexColumn(column.ColumnName, DatabaseIndexDefinition.SortOrder.Asc))
                .ToArray();
        }

        private static SortedDictionary<string, string> GetColumns(SqlConnection connection, dynamic dbTable, string schema)
        {
            var columns = connection.Query(
                @"
    SELECT 
        c.name as column_name
        , o.name
        , columnproperty(c.object_id, c.name, 'charmaxlen') as max_length
        , c.is_nullable
        , c.is_identity
        , (select top 1 t.name from sys.types t where t.system_type_id= c.system_type_id) as data_type
        , (select top 1 d.definition from sys.default_constraints d where d.object_id = c.default_object_id) as default_value
    from sys.columns c
        join sys.objects o on c.object_id = o.object_id
        join sys.schemas s on o.schema_id = s.schema_id
    WHERE o.name = @table_name and s.name = @table_schema",
                new Dictionary<string, string>()
                { { "table_schema", schema}, {"TABLE_NAME", dbTable.TABLE_NAME }})
                .ToDictionary(
                    x => (string)x.column_name,
                    x =>
                        new DatabaseTable.ColumnDefinition(DatabaseTable.ColumnDefinition.GetColumnType(x.data_type, x.max_length),
                            x.is_nullable, x.default_value, x.is_identity).ToString());
            return new SortedDictionary<string, string>(columns);
        }

        public SortedDictionary<string, DatabaseTable> Tables { get; set; } = new SortedDictionary<string, DatabaseTable>();

        public static string[] Compare(DatabaseSchema source, DatabaseSchema target, string sourceName, string targetName)
        {
            return source.Tables.CompareTo(target.Tables, "Table", sourceName, targetName,
                (sourceTable, targetTable, tableName) => DatabaseTable.Compare(sourceTable, targetTable, tableName, sourceName, targetName));
        }

        public string ToSql(string schemaName)
        {
            StringBuilder builder = new StringBuilder();

            builder.Append($@"
    GO");

            // First build the tables and indexes
            foreach (var table in Tables)
            {
                builder.Append(table.Value.ToSql(schemaName, table.Key));
            }

            // Then build all foreign keys
            foreach (var table in Tables)
            {
                builder.Append(table.Value.ForeignKeysToSql(schemaName, table.Key));
            }

            return builder.ToString();
        }

        public delegate string[] CompareItem<in T>(T source, T target, string name);

    }



}
