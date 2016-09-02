namespace SqlStreamStore.MsSqlScripts
{
    using System;
    using System.Collections.Generic;
    using System.Linq;

    public class DatabaseTable
    {
        private string[] _foreignKeys;
        private string[] _indexes;
        public SortedDictionary<string, string> Columns { get; set; } = new SortedDictionary<string, string>();

        public static string[] Compare(DatabaseTable source, DatabaseTable target, string tableName, string sourceName,
            string targetName)
        {
            // method to compare columns:
            return CompareColumns(source, target, tableName, sourceName, targetName)
                .Union(CompareForeignKeys(source, target, tableName, sourceName, targetName))
                .Union(CompareIndexes(source, target, tableName, sourceName, targetName))
                .ToArray();
        }
        private static string[] CompareIndexes(DatabaseTable sourceTable, DatabaseTable targetTable, string tableName, string sourceName, string targetName)
        {
            return sourceTable
                .Indexes.Sort()
                .CompareTo(
                    targetTable.Indexes.Sort(),
                    $"Index in table '{tableName}'", sourceName, targetName);
        }

        private static string[] CompareForeignKeys(DatabaseTable sourceTable, DatabaseTable targetTable, string tableName, string sourceName, string targetName)
        {
            return sourceTable
                .ForeignKeys.Sort()
                .CompareTo(
                    targetTable.ForeignKeys.Sort(),
                    $"Foreign Key in table '{tableName}'", sourceName, targetName);
        }

        private static string[] CompareColumns(DatabaseTable sourceTable, DatabaseTable targetTable, string tableName, string sourceName,
            string targetName)
        {
            DatabaseSchema.CompareItem<string> compare = (source, target, name) =>
                // column definition must equal
                string.Equals(source, target, StringComparison.OrdinalIgnoreCase)
                    // no differences
                    ? new string[0]
                    // different:
                    : new string[]
                    {
                        $"Column '{name}' in '{targetName}' was different in '{sourceName}' ({source}) than '{targetName}' ({target}) "
                    };


            return sourceTable
                .Columns.CompareTo(
                targetTable.Columns, $"Column in table '{tableName}'", sourceName, targetName, compare);
        }

        public string[] ForeignKeys
        {
            get { return _foreignKeys; }
            set
            {
                if (value == null || !value.Any())
                {
                    _foreignKeys = null;
                }
                else
                {
                    _foreignKeys = value;
                }
            }
        }

        public string[] Indexes
        {
            get { return _indexes; }
            set
            {
                if (value == null || !value.Any())
                {
                    _foreignKeys = null;
                }
                else
                {
                    _indexes = value;
                }
            }
        }

        public string ToSql(string schemaName, string tableName)
        {
            return $@"
    CREATE TABLE [{schemaName}].[{tableName}]
    (
        {BuildColumnsSql()}
    )
    GO{BuildIndexes(schemaName, tableName)}
    ";
        }

        private string BuildIndexes(string schemaName, string tableName)
        {
            return this.Indexes?
                .Select((indexDefinition, i) => DatabaseIndexDefinition.Parse(indexDefinition).ToSql(schemaName, tableName, i))
                .ToStringJoined("\r\n");
        }

        public string ForeignKeysToSql(string schemaName, string tableName)
        {
            return this.ForeignKeys?
                .Select((foreignKey, i) => DatabaseForeignKey.Parse(foreignKey).ToSql(schemaName, tableName, i))
                .ToStringJoined("\r\n");
        }

        private string BuildColumnsSql()
        {
            return Columns.Select(x => ColumnDefinition.Parse(x.Value).ToSql(x.Key)).ToStringJoined(",\r\n        ");
        }

        //public string[] CompareTo(string sourceTable, Table targetTable, string targetName)
        //{

        //}

        public class ColumnDefinition
        {
            public readonly string Type;
            public readonly bool Nullable;
            public readonly string DefaultValue;
            public readonly bool Identity;

            public static string GetColumnType(string datatype, int? maxLength)
            {
                if (maxLength != null)
                {
                    string maxLengthValue = maxLength.ToString();
                    if (maxLength == -1)
                    {
                        maxLengthValue = "max";
                    }
                    datatype = $"{datatype}({maxLengthValue})";
                }
                return datatype;
            }

            public ColumnDefinition(string type, bool? nullable, string defaultValue, bool? identity)
            {
                Type = type;
                Nullable = nullable.GetValueOrDefault();
                Identity = identity.GetValueOrDefault();
                if (!string.IsNullOrEmpty(defaultValue))
                {
                    DefaultValue = defaultValue;
                    if (!defaultValue.StartsWith("default"))
                    {
                        DefaultValue = "default" + defaultValue;
                    }
                }
            }

            public override string ToString()
            {
                List<string> parts = new List<string>();
                parts.Add(Type);
                if (Nullable)
                {
                    parts.Add("Null");
                }
                if (!string.IsNullOrEmpty(DefaultValue))
                {
                    parts.Add(DefaultValue);
                }
                if (Identity)
                {
                    parts.Add("Identity");
                }
                return parts.ToStringJoined(" ");
            }

            public static ColumnDefinition Parse(string input)
            {
                var parts = input.Split(' ');
                bool nullable = parts.Contains("null", StringComparer.OrdinalIgnoreCase);
                bool identity = parts.Contains("identity", StringComparer.OrdinalIgnoreCase);
                string defaultValue = parts.FirstOrDefault(x => x.StartsWith("default(", StringComparison.OrdinalIgnoreCase));
                return new ColumnDefinition(parts[0], nullable, defaultValue, identity);
            }

            public string ToSql(string name)
            {
                var nullable = this.Nullable ? " NULL" : " NOT NULL";
                var defaultValue = string.IsNullOrEmpty(this.DefaultValue) ? "" : " " + this.DefaultValue;
                var identity = this.Identity ? " Identity(1, 1)" : "";
                return $"[{name}] {Type}{identity}{nullable}{defaultValue}";
            }
        }
    }
}