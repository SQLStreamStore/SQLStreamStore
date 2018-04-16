namespace SqlStreamStore.PgSqlScripts
{
    using System;
    using Npgsql;
    using NpgsqlTypes;
    using SqlStreamStore.Streams;
    using StreamStoreStore.Json;

    internal static class Parameters
    {
        private const int StreamIdSize = 42;
        private const int OriginalStreamIdSize = 1000;

        public static NpgsqlParameter StreamId(PostgresqlStreamId value) => new NpgsqlParameter
        {
            NpgsqlDbType = NpgsqlDbType.Char,
            Size = StreamIdSize,
            NpgsqlValue = value.Id
        };

        public static NpgsqlParameter StreamIdOriginal(PostgresqlStreamId value) => new NpgsqlParameter
        {
            NpgsqlDbType = NpgsqlDbType.Varchar,
            Size = OriginalStreamIdSize,
            NpgsqlValue = value.IdOriginal
        };

        public static NpgsqlParameter MetadataStreamId(PostgresqlStreamId value) => new NpgsqlParameter
        {
            NpgsqlDbType = NpgsqlDbType.Char,
            Size = StreamIdSize,
            NpgsqlValue = value.Id
        };

        public static NpgsqlParameter MetadataStreamIdOriginal(PostgresqlStreamId value) => new NpgsqlParameter
        {
            NpgsqlDbType = NpgsqlDbType.Char,
            Size = StreamIdSize,
            NpgsqlValue = value.IdOriginal
        };

        public static NpgsqlParameter DeletedStreamId => new NpgsqlParameter
        {
            NpgsqlDbType = NpgsqlDbType.Char,
            Size = StreamIdSize,
            NpgsqlValue = PostgresqlStreamId.Deleted.Id
        };

        public static NpgsqlParameter DeletedStreamIdOriginal => new NpgsqlParameter
        {
            NpgsqlDbType = NpgsqlDbType.Varchar,
            Size = OriginalStreamIdSize,
            NpgsqlValue = PostgresqlStreamId.Deleted.IdOriginal
        };

        public static NpgsqlParameter DeletedMessages(PostgresqlStreamId streamId, params Guid[] messageIds) =>
            new NpgsqlParameter
            {
                NpgsqlValue = Array.ConvertAll(
                    messageIds,
                    messageId => PostgresNewStreamMessage.FromNewStreamMessage(
                        Deleted.CreateMessageDeletedMessage(streamId.IdOriginal, messageId))
                )
            };

        public static NpgsqlParameter DeletedStreamMessage(PostgresqlStreamId streamId) =>
            new NpgsqlParameter
            {
                NpgsqlValue = PostgresNewStreamMessage.FromNewStreamMessage(
                    Deleted.CreateStreamDeletedMessage(streamId.IdOriginal))
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

        public static NpgsqlParameter MetadataStreamMessage(MetadataMessage value) => new NpgsqlParameter
        {
            NpgsqlValue = PostgresNewStreamMessage.FromNewStreamMessage(
                new NewStreamMessage(
                    Guid.NewGuid(),
                    "$stream-metadata",
                    SimpleJson.SerializeObject(value)))
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

        public static NpgsqlParameter Prefetch(bool value) => new NpgsqlParameter
        {
            NpgsqlDbType = NpgsqlDbType.Boolean,
            NpgsqlValue = value
        };

        public static NpgsqlParameter MessageIds(Guid[] value) => new NpgsqlParameter
        {
            Value = value
        };

        public static NpgsqlParameter Position(long value) => new NpgsqlParameter
        {
            NpgsqlDbType = NpgsqlDbType.Bigint,
            NpgsqlValue = value
        };

        public static NpgsqlParameter OptionalMaxAge(int? value) => new NpgsqlParameter
        {
            NpgsqlDbType = NpgsqlDbType.Integer,
            NpgsqlValue = value.HasValue ? (object) value.Value : DBNull.Value
        };

        public static NpgsqlParameter OptionalMaxCount(int? value) => new NpgsqlParameter
        {
            NpgsqlDbType = NpgsqlDbType.Integer,
            NpgsqlValue = value.HasValue ? (object) value.Value : DBNull.Value
        };
    }
}