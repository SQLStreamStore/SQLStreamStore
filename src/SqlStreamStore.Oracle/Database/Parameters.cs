namespace SqlStreamStore.Oracle.Database
{
    using System;
    using System.Data;
    using global::Oracle.ManagedDataAccess.Client;
    using SqlStreamStore.Streams;

    internal static class Parameters
    {
        private const int StreamIdSize = 42;
        private const int OriginalStreamIdSize = 1000;

        // public static OracleParameter DeletedStreamId => new OracleParameter
        // {
        //     NpgsqlDbType = NpgsqlDbType.Char,
        //     Size = StreamIdSize,
        //     TypedValue = PostgresqlStreamId.Deleted.Id
        // };
        //
        // public static NpgsqlParameter DeletedStreamIdOriginal => new NpgsqlParameter<string>
        // {
        //     NpgsqlDbType = NpgsqlDbType.Varchar,
        //     Size = OriginalStreamIdSize,
        //     TypedValue = PostgresqlStreamId.Deleted.IdOriginal
        // };

        public static OracleParameter StreamId(OracleStreamId value)
        {
            return new OracleParameter("StreamId", OracleDbType.Char, StreamIdSize, value.Id, ParameterDirection.Input);
        }

        public static OracleParameter StreamIdOriginal(OracleStreamId value)
        {
            return new OracleParameter("StreamIdOriginal", OracleDbType.NVarchar2, OriginalStreamIdSize, value.IdOriginal, ParameterDirection.Input);
        }

        public static OracleParameter MetadataStreamId(OracleStreamId value)
        {
            return new OracleParameter("MetaStreamId", OracleDbType.Char, StreamIdSize, value.Id, ParameterDirection.Input);
        }
        
        public static OracleParameter MetadataStreamIdOriginal(OracleStreamId value)
        {
            return new OracleParameter("MetaStreamIdOriginal", OracleDbType.NVarchar2, OriginalStreamIdSize, value.IdOriginal, ParameterDirection.Input);
        }
        
        public static OracleParameter ExpectedVersion(int value)
        {
            return new OracleParameter("ExpectedVersion", OracleDbType.Int16, value, ParameterDirection.Input);
        }
        
        public static OracleParameter EventId(Guid value)
        {
            return new OracleParameter("EventId", OracleDbType.Varchar2, 40, value.ToString("N"), ParameterDirection.Input);
        }
        
        public static OracleParameter NewStreamMessages(NewStreamMessage[] value)
        {
            return new OracleParameter()
            {
                CollectionType = OracleCollectionType.PLSQLAssociativeArray,
                Size = value.Length,
                
            };
        }
        
        // public static NpgsqlParameter DeletedMessages(PostgresqlStreamId streamId, params Guid[] messageIds)
        // {
        //     return new NpgsqlParameter<PostgresNewStreamMessage[]>
        //     {
        //         TypedValue = Array.ConvertAll(
        //             messageIds,
        //             messageId => PostgresNewStreamMessage.FromNewStreamMessage(
        //                 Deleted.CreateMessageDeletedMessage(streamId.IdOriginal, messageId))
        //         )
        //     };
        // }
        //
        // public static NpgsqlParameter DeletedStreamMessage(PostgresqlStreamId streamId)
        // {
        //     return new NpgsqlParameter<PostgresNewStreamMessage>
        //     {
        //         TypedValue = PostgresNewStreamMessage.FromNewStreamMessage(
        //             Deleted.CreateStreamDeletedMessage(streamId.IdOriginal))
        //     };
        // }
        //
        //
       
        //
       
        //
        // public static NpgsqlParameter MetadataStreamMessage(
        //     PostgresqlStreamId streamId,
        //     int expectedVersion,
        //     MetadataMessage value)
        // {
        //     var jsonData = SimpleJson.SerializeObject(value);
        //     return new NpgsqlParameter<PostgresNewStreamMessage>
        //     {
        //         TypedValue = PostgresNewStreamMessage.FromNewStreamMessage(
        //             new NewStreamMessage(
        //                 MetadataMessageIdGenerator.Create(streamId.IdOriginal, expectedVersion, jsonData),
        //                 "$stream-metadata",
        //                 jsonData))
        //     };
        // }
        //
        // public static NpgsqlParameter Count(int value)
        // {
        //     return new NpgsqlParameter<int>
        //     {
        //         NpgsqlDbType = NpgsqlDbType.Integer,
        //         TypedValue = value
        //     };
        // }
        //
        // public static NpgsqlParameter Version(int value)
        // {
        //     return new NpgsqlParameter<int>
        //     {
        //         NpgsqlDbType = NpgsqlDbType.Integer,
        //         TypedValue = value
        //     };
        // }
        //
        // public static NpgsqlParameter ReadDirection(ReadDirection direction)
        // {
        //     return new NpgsqlParameter<bool>
        //     {
        //         NpgsqlDbType = NpgsqlDbType.Boolean,
        //         TypedValue = direction == Streams.ReadDirection.Forward
        //     };
        // }
        //
        // public static NpgsqlParameter Prefetch(bool value)
        // {
        //     return new NpgsqlParameter<bool>
        //     {
        //         NpgsqlDbType = NpgsqlDbType.Boolean,
        //         TypedValue = value
        //     };
        // }
        //
        // public static NpgsqlParameter MessageIds(Guid[] value)
        // {
        //     return new NpgsqlParameter<Guid[]>
        //     {
        //         Value = value
        //     };
        // }
        //
        // public static NpgsqlParameter Position(long value)
        // {
        //     return new NpgsqlParameter<long>
        //     {
        //         NpgsqlDbType = NpgsqlDbType.Bigint,
        //         TypedValue = value
        //     };
        // }
        //
        // public static NpgsqlParameter OptionalMaxAge(int? value)
        // {
        //     return new NpgsqlParameter
        //     {
        //         NpgsqlDbType = NpgsqlDbType.Integer,
        //         NpgsqlValue = value.HasValue ? (object) value.Value : DBNull.Value
        //     };
        // }
        //
        // public static NpgsqlParameter OptionalMaxCount(int? value)
        // {
        //     return new NpgsqlParameter
        //     {
        //         NpgsqlDbType = NpgsqlDbType.Integer,
        //         NpgsqlValue = value.HasValue ? (object) value.Value : DBNull.Value
        //     };
        // }
        //
        // public static NpgsqlParameter MaxCount(int value)
        // {
        //     return new NpgsqlParameter<int>
        //     {
        //         NpgsqlDbType = NpgsqlDbType.Integer,
        //         TypedValue = value
        //     };
        // }
        //
        // public static NpgsqlParameter OptionalStartingAt(int? value)
        // {
        //     return value.HasValue
        //         ? (NpgsqlParameter) new NpgsqlParameter<int>
        //         {
        //             NpgsqlDbType = NpgsqlDbType.Integer,
        //             TypedValue = value.Value
        //         }
        //         : new NpgsqlParameter<DBNull>
        //         {
        //             TypedValue = DBNull.Value
        //         };
        // }
        //
        // public static NpgsqlParameter OptionalAfterIdInternal(int? value)
        // {
        //     return value.HasValue
        //         ? (NpgsqlParameter) new NpgsqlParameter<int>
        //         {
        //             NpgsqlDbType = NpgsqlDbType.Integer,
        //             TypedValue = value.Value
        //         }
        //         : new NpgsqlParameter<DBNull>
        //         {
        //             TypedValue = DBNull.Value
        //         };
        // }
        //
        // public static NpgsqlParameter Pattern(string value)
        // {
        //     return new NpgsqlParameter<string>
        //     {
        //         TypedValue = value,
        //         NpgsqlDbType = NpgsqlDbType.Varchar
        //     };
        // }
        //
        // public static NpgsqlParameter DeletionTrackingDisabled(bool deletionTrackingDisabled)
        // {
        //     return new NpgsqlParameter<bool>
        //     {
        //         TypedValue = deletionTrackingDisabled,
        //         NpgsqlDbType = NpgsqlDbType.Boolean
        //     };
        // }
        //
        // public static NpgsqlParameter Empty()
        // {
        //     return new NpgsqlParameter<DBNull>
        //     {
        //         TypedValue = DBNull.Value
        //     };
        // }

    }
}