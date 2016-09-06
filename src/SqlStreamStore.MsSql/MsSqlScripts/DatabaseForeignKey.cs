namespace SqlStreamStore.MsSqlScripts
{
    using System;

    public class DatabaseForeignKey
    {
        public class ColumnMapping
        {
            public readonly string ColumnInSourceTable;
            public readonly string ColumnInTargetTable;

            public ColumnMapping(string columnInSourceTable, string columnInTargetTable)
            {
                ColumnInSourceTable = columnInSourceTable;
                ColumnInTargetTable = columnInTargetTable;
            }

            public static ColumnMapping Parse(string input)
            {
                var parts = input.Split(new[] { "->" }, StringSplitOptions.RemoveEmptyEntries);
                if (parts.Length != 2)
                {
                    throw new InvalidOperationException($"Columnmapping '{input}' should contain exactly one '->' to separate source from target. IE: 'column1->column2'");
                }
                return new ColumnMapping(parts[0].Trim(), parts[1].Trim());
            }

            public override string ToString()
            {
                return $"{ColumnInSourceTable}->{ColumnInTargetTable}";
            }
        }

        public readonly string TargetTable;
        public readonly ColumnMapping Column;

        public DatabaseForeignKey(string targetTable, ColumnMapping column)
        {
            TargetTable = targetTable;
            Column = column;
        }

        public static DatabaseForeignKey Parse(string input)
        {
            var parts = input.TrimEnd(')').Split('(');
            if (parts.Length != 2)
            {
                throw new InvalidOperationException($"Foreign key '{input}' is not in valid format. Failed to distinguish tablename from columns Format should be: tableName(column1:column2,column3:column4)");
            }
            var tableName = parts[0];
            var column = ColumnMapping.Parse(parts[1]);

            return new DatabaseForeignKey(tableName, column);
        }

        public override string ToString()
        {
            return $"{TargetTable}({Column.ToString()})";
        }

        public string ToSql(object schemaName, object tableName, int i)
        {
            return $@"
    ALTER TABLE [{schemaName}].[{tableName}]
        WITH CHECK ADD CONSTRAINT [FK_{schemaName}_{tableName}_{TargetTable}({i})] FOREIGN KEY ({Column.ColumnInSourceTable})
        REFERENCES [{schemaName}].[{TargetTable}]({Column.ColumnInTargetTable})
    ";
        }
    }
}