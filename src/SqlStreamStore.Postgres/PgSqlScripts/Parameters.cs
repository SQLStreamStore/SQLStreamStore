namespace SqlStreamStore.PgSqlScripts
{
    using System;
    using Npgsql;
    using NpgsqlTypes;
    using SqlStreamStore.Streams;

    internal static class Parameters
    {
        public static NpgsqlParameter StreamId(PostgresqlStreamId value) => new NpgsqlParameter
        {
            NpgsqlDbType = NpgsqlDbType.Char,
            Size = 42,
            NpgsqlValue = value.Id 
        };

        public static NpgsqlParameter StreamIdOriginal(PostgresqlStreamId value) => new NpgsqlParameter
        {
            NpgsqlDbType = NpgsqlDbType.Varchar,
            Size = 1000,
            NpgsqlValue = value.IdOriginal 
        };

        public static NpgsqlParameter ExpectedVersion(int value) => new NpgsqlParameter
        {
            NpgsqlDbType = NpgsqlDbType.Integer,
            NpgsqlValue = value
        };

        public static NpgsqlParameter CreatedUtc(DateTime value) => new NpgsqlParameter
        {
            NpgsqlValue = value,
            NpgsqlDbType = NpgsqlDbType.Timestamp
        };

        public static NpgsqlParameter NewStreamMessages(NewStreamMessage[] value) => new NpgsqlParameter
        {
            NpgsqlValue = Array.ConvertAll(value, PostgresNewStreamMessage.FromNewStreamMessage)
        };
        
        public static NpgsqlParameter Count(int value) => new NpgsqlParameter
        {
            NpgsqlDbType = NpgsqlDbType.Integer,
            NpgsqlValue = value
        };
        
        public static NpgsqlParameter Version(int value) => new NpgsqlParameter
        {
            NpgsqlDbType = NpgsqlDbType.Integer,
            NpgsqlValue = value
        };
        
        public static NpgsqlParameter ReadDirection(ReadDirection direction) => new NpgsqlParameter
        {
            NpgsqlDbType = NpgsqlDbType.Boolean,
            NpgsqlValue = direction == Streams.ReadDirection.Forward
        };

        public static NpgsqlParameter Prefetch(bool value)=> new NpgsqlParameter
        {
            NpgsqlDbType = NpgsqlDbType.Boolean,
            NpgsqlValue = value
        };

        public static NpgsqlParameter MessageId(Guid value) => new NpgsqlParameter
        {
            NpgsqlDbType = NpgsqlDbType.Uuid,
            Value = value
        };

        public static NpgsqlParameter Position(long value) => new NpgsqlParameter
        {
            NpgsqlDbType = NpgsqlDbType.Bigint,
            NpgsqlValue = value
        };
    }
}