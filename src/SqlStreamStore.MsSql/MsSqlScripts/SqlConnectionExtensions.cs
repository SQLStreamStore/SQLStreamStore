namespace SqlStreamStore.MsSqlScripts
{
    using System.Collections.Generic;
    using System.Data;
    using System.Data.SqlClient;
    using System.Dynamic;
    using System.Linq;

    internal static class SqlConnectionExtensions
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
}