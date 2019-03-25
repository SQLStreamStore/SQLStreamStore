namespace SqlStreamStore.MySqlScripts
{
    using System;
    using System.Data;
    using MySql.Data.MySqlClient;
    using SqlStreamStore.Infrastructure;
    using SqlStreamStore.Streams;

    internal static class Parameters
    {
        private const int StreamIdSize = 42;
        private const int StreamIdOriginalSize = 1000;

        public static MySqlParameter StreamId(MySqlStreamId streamId)
            => StreamIdInternal(streamId, "_stream_id");

        public static MySqlParameter StreamIdOriginal(MySqlStreamId streamId)
            => StreamIdOriginalInternal(streamId, "_stream_id_original");

        public static MySqlParameter ExpectedVersion(int expectedVersion)
            => new MySqlParameter
            {
                MySqlDbType = MySqlDbType.Int32,
                Value = expectedVersion,
                ParameterName = "_expected_version"
            };

        public static MySqlParameter CreatedUtc(DateTime? createdUtc)
            => new MySqlParameter
            {
                MySqlDbType = MySqlDbType.DateTime,
                Value = createdUtc.HasValue ? (object) createdUtc.Value : DBNull.Value,
                Precision = 6,
                ParameterName = "_created_utc"
            };

        public static MySqlParameter MessageId(Guid messageId)
            => MessageIdInternal(messageId, "_message_id");

        public static MySqlParameter Type(string type)
            => TypeInternal(type, "_type");

        public static MySqlParameter JsonData(string jsonData)
            => JsonDataInternal(jsonData, "_json_data");

        public static MySqlParameter JsonMetadata(string jsonMetadata)
            => JsonMetadataInternal(jsonMetadata, "_json_metadata");

        public static MySqlParameter CurrentVersion()
            => new MySqlParameter
            {
                MySqlDbType = MySqlDbType.Int32,
                Direction = ParameterDirection.Output,
                ParameterName = "_current_version"
            };

        public static MySqlParameter CurrentPosition()
            => new MySqlParameter
            {
                MySqlDbType = MySqlDbType.Int64,
                Direction = ParameterDirection.Output,
                ParameterName = "_current_position"
            };

        public static MySqlParameter Count(int count)
            => new MySqlParameter
            {
                MySqlDbType = MySqlDbType.Int32,
                Value = count,
                ParameterName = "_count"
            };

        public static MySqlParameter Version(int version)
            => new MySqlParameter
            {
                MySqlDbType = MySqlDbType.Int32,
                Value = version,
                ParameterName = "_version"
            };

        public static MySqlParameter ReadDirection(ReadDirection readDirection)
            => new MySqlParameter
            {
                MySqlDbType = MySqlDbType.Bool,
                Value = readDirection == Streams.ReadDirection.Forward,
                ParameterName = "_forwards"
            };

        public static MySqlParameter Prefetch(bool prefetch)
            => new MySqlParameter
            {
                MySqlDbType = MySqlDbType.Bool,
                Value = prefetch,
                ParameterName = "_prefetch"
            };

        public static MySqlParameter Position(long position)
            => new MySqlParameter
            {
                MySqlDbType = MySqlDbType.Int64,
                Value = position,
                ParameterName = "_position"
            };

        public static MySqlParameter DeletedStreamId()
            => StreamIdInternal(StreamIdInfo.Deleted.MySqlStreamId, "_deleted_stream_id");

        public static MySqlParameter DeletedStreamIdOriginal()
            => StreamIdOriginalInternal(StreamIdInfo.Deleted.MySqlStreamId, "_deleted_stream_id_original");

        public static MySqlParameter DeletedMetadataStreamId()
            => StreamIdInternal(StreamIdInfo.Deleted.MetadataMySqlStreamId, "_deleted_metadata_stream_id");

        public static MySqlParameter DeletedStreamMessageMessageId(NewStreamMessage deletedStreamMessage)
            => MessageIdInternal(deletedStreamMessage.MessageId, "_deleted_stream_message_message_id");

        public static MySqlParameter DeletedStreamMessageType(NewStreamMessage deletedStreamMessage)
            => TypeInternal(deletedStreamMessage.Type, "_deleted_stream_message_type");

        public static MySqlParameter DeletedStreamMessageJsonData(NewStreamMessage deletedStreamMessage)
            => JsonDataInternal(deletedStreamMessage.JsonData, "_deleted_stream_message_json_data");

        public static MySqlParameter DeletedMessageMessageId(NewStreamMessage deletedStreamMessage)
            => MessageIdInternal(deletedStreamMessage.MessageId, "_deleted_message_message_id");

        public static MySqlParameter DeletedMessageType(NewStreamMessage deletedStreamMessage)
            => TypeInternal(deletedStreamMessage.Type, "_deleted_message_type");

        public static MySqlParameter DeletedMessageJsonData(NewStreamMessage deletedStreamMessage)
            => JsonDataInternal(deletedStreamMessage.JsonData, "_deleted_message_json_data");

        public static MySqlParameter MetadataStreamId(MySqlStreamId streamId)
            => StreamIdInternal(streamId, "_metadata_stream_id");

        public static MySqlParameter MetadataStreamIdOriginal(MySqlStreamId streamId)
            => StreamIdOriginalInternal(streamId, "_metadata_stream_id_original");

        public static MySqlParameter MetadataMessageMessageId(
            MySqlStreamId metadataMySqlStreamId,
            int expectedStreamMetadataVersion,
            string metadataMessageJsonData) => MessageIdInternal(
            MetadataMessageIdGenerator.Create(
                metadataMySqlStreamId.IdOriginal,
                expectedStreamMetadataVersion,
                metadataMessageJsonData),
            "_metadata_message_message_id");

        public static MySqlParameter MetadataMessageType()
            => TypeInternal("$stream-metadata", "_metadata_message_type");

        public static MySqlParameter MetadataMessageJsonData(string metadataMessageJsonData)
            => JsonDataInternal(metadataMessageJsonData, "_metadata_message_json_data");

        public static MySqlParameter OptionalMaxAge(int? metadataMaxAge)
            => new MySqlParameter
            {
                MySqlDbType = MySqlDbType.Int32,
                Value = metadataMaxAge.HasValue ? (object) metadataMaxAge.Value : DBNull.Value,
                ParameterName = "_max_age"
            };

        public static MySqlParameter OptionalMaxCount(int? metadataMaxCount)
            => new MySqlParameter
            {
                MySqlDbType = MySqlDbType.Int32,
                Value = metadataMaxCount.HasValue ? (object) metadataMaxCount.Value : DBNull.Value,
                ParameterName = "_max_count"
            };

        public static MySqlParameter MaxCount(int maxCount)
            => new MySqlParameter
            {
                MySqlDbType = MySqlDbType.Int32,
                Value = maxCount,
                ParameterName = "_max_count"
            };

        public static MySqlParameter OptionalAfterIdInternal(int? afterIdInternal)
            => new MySqlParameter
            {
                MySqlDbType = MySqlDbType.Int32,
                Value = afterIdInternal.HasValue ? (object) afterIdInternal.Value : DBNull.Value,
                ParameterName = "_after_id_internal"
            };

        public static MySqlParameter Pattern(string patternValue)
            => new MySqlParameter
            {
                MySqlDbType = MySqlDbType.VarChar,
                Size = 1000,
                Value = patternValue,
                ParameterName = "_pattern"
            };

        private static MySqlParameter StreamIdInternal(MySqlStreamId streamId, string parameterName)
            => new MySqlParameter
            {
                MySqlDbType = MySqlDbType.String,
                Size = StreamIdSize,
                Value = streamId.Id,
                ParameterName = parameterName
            };

        private static MySqlParameter StreamIdOriginalInternal(MySqlStreamId streamId, string parameterName)
            => new MySqlParameter
            {
                MySqlDbType = MySqlDbType.String,
                Size = StreamIdOriginalSize,
                Value = streamId.IdOriginal,
                ParameterName = parameterName
            };

        private static MySqlParameter MessageIdInternal(Guid messageId, string parameterName)
            => new MySqlParameter
            {
                MySqlDbType = MySqlDbType.Guid,
                Value = messageId,
                ParameterName = parameterName
            };

        private static MySqlParameter TypeInternal(string type, string parameterName)
            => new MySqlParameter
            {
                MySqlDbType = MySqlDbType.VarString,
                Size = 128,
                Value = type,
                ParameterName = parameterName
            };

        private static MySqlParameter JsonDataInternal(string jsonData, string parameterName)
            => new MySqlParameter
            {
                MySqlDbType = MySqlDbType.LongText,
                Value = jsonData,
                ParameterName = parameterName
            };

        private static MySqlParameter JsonMetadataInternal(string jsonMetadata, string parameterName)
            => new MySqlParameter
            {
                MySqlDbType = MySqlDbType.LongText,
                Value = jsonMetadata,
                ParameterName = parameterName
            };
    }
}