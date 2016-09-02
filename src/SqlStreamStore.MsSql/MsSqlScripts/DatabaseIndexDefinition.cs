namespace SqlStreamStore.MsSqlScripts
{
    using System;
    using System.Linq;

    public class DatabaseIndexDefinition
    {
        public readonly bool Clustered;
        public readonly IndexType Type;
        public readonly IndexColumn[] Columns;
        public readonly IndexColumn[] IncludedOnlyColumns;

        public enum IndexType
        {
            Index = 0,
            Unique = 1,
            PrimaryKey = 2
        }

        public DatabaseIndexDefinition(IndexType type, bool clustered, IndexColumn[] columns, IndexColumn[] includedOnlyColumns)
        {
            Type = type;
            Clustered = clustered;
            Columns = columns;
            IncludedOnlyColumns = includedOnlyColumns;
        }

        public static IndexType GetType(bool? isPrimaryKey, bool? isUnique)
        {
            if (isPrimaryKey.GetValueOrDefault())
            {
                return IndexType.PrimaryKey;
            }
            if (isUnique.GetValueOrDefault())
            {
                return IndexType.Unique;
            }
            return IndexType.Index;

        }

        public static DatabaseIndexDefinition Parse(string input)
        {
            var parts = input.Split('{');
            if (parts.Length == 0 || parts.Length > 2)
            {
                throw new InvalidOperationException($"Index definition {input} invalid. Valid example is: Unique(column1, column2){{includedcolumn1, includedcolumn2}}.");
            }
            var requiredParts = parts[0].Trim().TrimEnd(')').Split('(');

            if (requiredParts.Length != 2)
            {
                throw new InvalidOperationException($"Index definition {input} invalid. Valid example is: Unique(column1, column2){{includedcolumn1, includedcolumn2}}.");
            }

            var unique = requiredParts[0].StartsWith("unique", StringComparison.OrdinalIgnoreCase);
            var primarykey = requiredParts[0].StartsWith("primarykey", StringComparison.OrdinalIgnoreCase);
            var index = requiredParts[0].StartsWith("index", StringComparison.OrdinalIgnoreCase);
            var clustered = requiredParts[0].EndsWith(":clustered", StringComparison.OrdinalIgnoreCase);
            if (!unique && !primarykey && !index)
            {
                throw new InvalidOperationException($"Invalid type of index {requiredParts[0]}. Supported types are 'Unique', 'Index' ,'Unique', and can be appended with :Clustered ");
            }

            var type = IndexType.Index;
            if (unique)
            {
                type = IndexType.Unique;
            }
            else if (primarykey)
            {
                type = IndexType.PrimaryKey;
            }

            var columns = requiredParts[1].Split(',').Select(IndexColumn.Parse).ToArray();
            IndexColumn[] includedColumns = null;
            if (parts.Length == 2)
            {
                includedColumns = parts[1].TrimEnd('}').Split(',').Select(IndexColumn.Parse).ToArray();
            }
            return new DatabaseIndexDefinition(type, clustered, columns, includedColumns);
        }

        public override string ToString()
        {
            string type = "";
            if (Type == IndexType.PrimaryKey)
            {
                type = "PrimaryKey";
            }
            else if (Type == IndexType.Unique)
            {
                type = "Unique";
            }
            else
            {
                type = "Index";
            }
            if (Clustered)
            {
                type += ":Clustered";
            }
            var columns = Columns.Select(x => x.ToString()).ToStringJoined();

            var indexedColumns = IncludedOnlyColumns?.Select(x => x.ToString()).ToStringJoined();
            if (!string.IsNullOrEmpty(indexedColumns))
            {
                indexedColumns = $"{{{indexedColumns}}}";
            }
            return $"{type}({columns}){indexedColumns}";
        }

        public string ToSql(string schemaName, string tableName, int index)
        {
            var clusterOptions = this.Clustered ? "CLUSTERED" : "NONCLUSTERED";

            // build the list of regular index columns
            var indexColumns = Columns.Select(x => x.ToSql()).ToStringJoined();

            // build list of included only columns (but only if needed). 
            var includedColumns = this.IncludedOnlyColumns == null || !this.IncludedOnlyColumns.Any()
                ? ""
                : $@"
    INCLUDE 
    (
        {this.IncludedOnlyColumns.Select(x => x.ToSql())
                    .ToStringJoined()}
    )";

            switch (this.Type)
            {
                case IndexType.Index:
                    return $@"
    CREATE {clusterOptions} INDEX [UX_{schemaName}.{tableName}({index})] ON [{schemaName}].[{tableName}]
    (
        {indexColumns}
    ){includedColumns}
    GO";
                case IndexType.PrimaryKey:
                    return $@"
    ALTER TABLE [{schemaName}].[{tableName}]
        ADD CONSTRAINT [PK_{schemaName}_{tableName}] PRIMARY KEY ({Columns.Select(x => x.ToSql()).ToStringJoined()})
    GO
    ";
                case IndexType.Unique:
                    return $@"
    CREATE UNIQUE {clusterOptions} INDEX [IX_{schemaName}.{tableName}({index})] ON [{schemaName}].[{tableName}]
    (
        {indexColumns}
    ){includedColumns}
    GO";
                default:
                    throw new NotImplementedException($"unknown index type {this.Type}");
            }

        }

        public enum SortOrder
        {
            Asc = 0,
            Desc = 1
        }


        public class IndexColumn
        {
            public readonly string Name;
            public readonly SortOrder SortOrder;

            public IndexColumn(string name, SortOrder sortOrder)
            {
                Name = name;
                SortOrder = sortOrder;
            }

            public override string ToString()
            {
                string direction = "";
                if (SortOrder == SortOrder.Desc)
                {
                    direction = ":Desc";
                }
                return $"{Name}{direction}";
            }

            public static IndexColumn Parse(string input)
            {
                var parts = input.Split(':');
                SortOrder direction;
                if (parts.Length > 2)
                {
                    throw new InvalidOperationException($"Column mapping for index was invalid {input}. Should have maximum 1 ':' character. Example: column1:desc");
                }

                if (parts.Length == 1)
                {
                    direction = SortOrder.Asc;
                }
                else if (string.Equals(parts[1], "Desc", StringComparison.OrdinalIgnoreCase))
                {
                    direction = SortOrder.Desc;
                }
                else if (string.Equals(parts[1], "Asc", StringComparison.OrdinalIgnoreCase))
                {
                    direction = SortOrder.Asc;
                }
                else
                {
                    throw new InvalidOperationException($"Unknown sort order: {parts[1]}");
                }
                return new IndexColumn(parts[0], direction);
            }

            public string ToSql()
            {
                string order = this.SortOrder == SortOrder.Asc ? "" : " DESC";
                return $"[{Name}]{order}";
            }
        }
    }
}