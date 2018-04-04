namespace SqlStreamStore.PgSqlScripts
{
    internal class Schema
    {
        private readonly string _schema;
        private readonly Scripts _scripts;

        public string Definition => _scripts.CreateSchema;
        public string DropAll => _scripts.DropAll;

        public string NewStreamMessage => $"{_schema}.new_stream_message";
        
        public string AppendToStream => $"{_schema}.append_to_stream";
        public string Scavenge => $"{_schema}.scavenge";
        public string DeleteStream => $"{_schema}.delete_stream";
        public string DeleteStreamMessages => $"{_schema}.delete_stream_messages";
        public string Read => $"{_schema}.read";
        public string ReadAll => $"{_schema}.read_all";
        public string ReadAllHeadPosition => $"{_schema}.read_head_position";
        public string ReadJsonData => $"{_schema}.read_json_data";
        public string ReadStreamMessageBeforeCreatedCount => $"{_schema}.read_stream_message_before_created_count";
        public string ReadStreamMessageCount => $"{_schema}.read_stream_message_count";
        public string ReadStreamVersionOfMessageId => $"{_schema}.read_stream_version_of_message_id";

        public static string FetchAll(string refcursor) => $@"FETCH ALL IN ""{refcursor}"";";

        public const string StreamsById = "streams_by_id";
        public const string MessagesByPosition = "messages_by_position";
        public const string MessagesByStreamIdInternalAndMessageId = "messages_by_stream_id_internal_and_message_id";
        public const string MessagesByStreamIdInternalAndStreamVersion = "messages_by_stream_id_internal_and_stream_version";
        public const string MessagesByStreamIdInternalAndCreatedUtc = "messages_by_stream_id_internal_and_created_utc";
        
        public Schema(string schema)
        {
            _schema = schema;
            _scripts = new Scripts(schema);
        }
    }
}