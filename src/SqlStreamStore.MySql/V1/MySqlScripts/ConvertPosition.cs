namespace SqlStreamStore.V1.MySqlScripts
{
    using MySql.Data.MySqlClient;
    using SqlStreamStore.V1.Streams;

    internal static class ConvertPosition
    {
        public static long FromMySqlToStreamStore(MySqlParameter positionParameter)
            => FromMySqlToStreamStore((long) positionParameter.Value);

        public static long FromMySqlToStreamStore(long position)
            => position == Position.End ? Position.End : position - 1;

        public static long FromStreamStoreToMySql(long position)
            => position >= Position.Start ? position + 1 : position;
    }
}