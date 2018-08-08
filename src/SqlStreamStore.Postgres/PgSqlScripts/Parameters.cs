﻿namespace SqlStreamStore.PgSqlScripts
{
    using System;
    using Npgsql;
    using NpgsqlTypes;
    using SqlStreamStore.Infrastructure;
    using SqlStreamStore.Streams;
    using StreamStoreStore.Json;

    internal static class Parameters
    {
        private const int StreamIdSize = 42;
        private const int OriginalStreamIdSize = 1000;

        public static NpgsqlParameter DeletedStreamId => new NpgsqlParameter<string>
        {
            NpgsqlDbType = NpgsqlDbType.Char,
            Size = StreamIdSize,
            TypedValue = PostgresqlStreamId.Deleted.Id
        };

        public static NpgsqlParameter DeletedStreamIdOriginal => new NpgsqlParameter<string>
        {
            NpgsqlDbType = NpgsqlDbType.Varchar,
            Size = OriginalStreamIdSize,
            TypedValue = PostgresqlStreamId.Deleted.IdOriginal
        };

        public static NpgsqlParameter StreamId(PostgresqlStreamId value)
        {
            return new NpgsqlParameter<string>
            {
                NpgsqlDbType = NpgsqlDbType.Char,
                Size = StreamIdSize,
                TypedValue = value.Id
            };
        }

        public static NpgsqlParameter StreamIdOriginal(PostgresqlStreamId value)
        {
            return new NpgsqlParameter<string>
            {
                NpgsqlDbType = NpgsqlDbType.Varchar,
                Size = OriginalStreamIdSize,
                TypedValue = value.IdOriginal
            };
        }

        public static NpgsqlParameter MetadataStreamId(PostgresqlStreamId value)
        {
            return new NpgsqlParameter<string>
            {
                NpgsqlDbType = NpgsqlDbType.Char,
                Size = StreamIdSize,
                TypedValue = value.Id
            };
        }

        public static NpgsqlParameter MetadataStreamIdOriginal(PostgresqlStreamId value)
        {
            return new NpgsqlParameter<string>
            {
                NpgsqlDbType = NpgsqlDbType.Char,
                Size = StreamIdSize,
                TypedValue = value.IdOriginal
            };
        }

        public static NpgsqlParameter DeletedMessages(PostgresqlStreamId streamId, params Guid[] messageIds)
        {
            return new NpgsqlParameter<PostgresNewStreamMessage[]>
            {
                TypedValue = Array.ConvertAll(
                    messageIds,
                    messageId => PostgresNewStreamMessage.FromNewStreamMessage(
                        Deleted.CreateMessageDeletedMessage(streamId.IdOriginal, messageId))
                )
            };
        }

        public static NpgsqlParameter DeletedStreamMessage(PostgresqlStreamId streamId)
        {
            return new NpgsqlParameter<PostgresNewStreamMessage>
            {
                TypedValue = PostgresNewStreamMessage.FromNewStreamMessage(
                    Deleted.CreateStreamDeletedMessage(streamId.IdOriginal))
            };
        }

        public static NpgsqlParameter ExpectedVersion(int value)
        {
            return new NpgsqlParameter<int>
            {
                NpgsqlDbType = NpgsqlDbType.Integer,
                TypedValue = value
            };
        }

        public static NpgsqlParameter CreatedUtc(DateTime value)
        {
            return new NpgsqlParameter<DateTime>
            {
                TypedValue = value,
                NpgsqlDbType = NpgsqlDbType.Timestamp
            };
        }

        public static NpgsqlParameter NewStreamMessages(NewStreamMessage[] value)
        {
            return new NpgsqlParameter<PostgresNewStreamMessage[]>
            {
                TypedValue = Array.ConvertAll(value, PostgresNewStreamMessage.FromNewStreamMessage)
            };
        }

        public static NpgsqlParameter MetadataStreamMessage(
            PostgresqlStreamId streamId,
            int expectedVersion,
            MetadataMessage value)
        {
            var jsonData = SimpleJson.SerializeObject(value);
            return new NpgsqlParameter<PostgresNewStreamMessage>
            {
                TypedValue = PostgresNewStreamMessage.FromNewStreamMessage(
                    new NewStreamMessage(
                        MetadataMessageIdGenerator.Create(streamId.IdOriginal, expectedVersion, jsonData),
                        "$stream-metadata",
                        jsonData))
            };
        }

        public static NpgsqlParameter Count(int value)
        {
            return new NpgsqlParameter<int>
            {
                NpgsqlDbType = NpgsqlDbType.Integer,
                TypedValue = value
            };
        }

        public static NpgsqlParameter Version(int value)
        {
            return new NpgsqlParameter<int>
            {
                NpgsqlDbType = NpgsqlDbType.Integer,
                TypedValue = value
            };
        }

        public static NpgsqlParameter ReadDirection(ReadDirection direction)
        {
            return new NpgsqlParameter<bool>
            {
                NpgsqlDbType = NpgsqlDbType.Boolean,
                TypedValue = direction == Streams.ReadDirection.Forward
            };
        }

        public static NpgsqlParameter Prefetch(bool value)
        {
            return new NpgsqlParameter<bool>
            {
                NpgsqlDbType = NpgsqlDbType.Boolean,
                TypedValue = value
            };
        }

        public static NpgsqlParameter MessageIds(Guid[] value)
        {
            return new NpgsqlParameter<Guid[]>
            {
                Value = value
            };
        }

        public static NpgsqlParameter Position(long value)
        {
            return new NpgsqlParameter<long>
            {
                NpgsqlDbType = NpgsqlDbType.Bigint,
                TypedValue = value
            };
        }

        public static NpgsqlParameter OptionalMaxAge(int? value)
        {
            return new NpgsqlParameter
            {
                NpgsqlDbType = NpgsqlDbType.Integer,
                NpgsqlValue = value.HasValue ? (object) value.Value : DBNull.Value
            };
        }

        public static NpgsqlParameter OptionalMaxCount(int? value)
        {
            return new NpgsqlParameter
            {
                NpgsqlDbType = NpgsqlDbType.Integer,
                NpgsqlValue = value.HasValue ? (object) value.Value : DBNull.Value
            };
        }
    }
}