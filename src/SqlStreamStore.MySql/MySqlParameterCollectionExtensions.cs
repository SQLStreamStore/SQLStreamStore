namespace SqlStreamStore
{
    using MySql.Data.MySqlClient;

    internal static class MySqlParameterCollectionExtensions
    {
        public static void Add(this MySqlParameterCollection collection, string name, object value)
            => collection.AddWithValue(name, value);
    }
}