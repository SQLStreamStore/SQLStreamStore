namespace SqlStreamStore.SQLiteScripts
{
    using System;
    using System.Data;
    using System.Data.SQLite;

    internal static class Parameters
    {
        private const int StreamIdSize = 42;

        public static SQLiteParameter StreamId(SQLiteStreamId value)
        {
            return new SQLiteParameter("@streamId")
            {

                DbType = DbType.String,
                Size = StreamIdSize,
                Value = value.Id
            };
        }

        public static SQLiteParameter StreamIdOriginal(SQLiteStreamId value)
        {
            return new SQLiteParameter("@streamIdOriginal")
            {

                DbType = DbType.String,
                Size = StreamIdSize,
                Value = value.Id
            };
        }

        public static SQLiteParameter MetadataStreamId(SQLiteStreamId value)
        {
            return new SQLiteParameter("@metadataStreamId")
            {

                DbType = DbType.String,
                Size = StreamIdSize,
                Value = value.Id
            };
        }

        public static SQLiteParameter ExpectedVersion(int value)
        {
            return new SQLiteParameter("@metadataStreamId")
            {

                DbType = DbType.Int32,
                Value = value
            };
        }

        public static SQLiteParameter CreatedUtc(DateTime? value)
        {
            return value.HasValue
                ? new SQLiteParameter("@createdUtc")
                {
                    DbType = DbType.DateTime,
                    Value = DBNull.Value
                }
                : new SQLiteParameter("@createdUtc")
                {
                    DbType = DbType.DateTime,
                    Value = value.Value
                };
        }
    }
}