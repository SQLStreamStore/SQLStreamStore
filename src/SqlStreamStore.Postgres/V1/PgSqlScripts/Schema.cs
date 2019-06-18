namespace SqlStreamStore.V1.PgSqlScripts
{
    internal class Schema
    {
        private readonly string _schema;
        private readonly Scripts _scripts;

        public string Definition => _scripts.CreateSchema;
        public string DropAll => _scripts.DropAll;
        public string EnableExplainAnalyze => _scripts.EnableExplainAnalyze;

        public string NewStreamMessage => $"{_schema}.new_stream_message";

        public string AppendToStream => $"{_schema}.append_to_stream";
        public string Scavenge => $"{_schema}.scavenge";
        public string SetStreamMetadata => $"{_schema}.set_stream_metadata";
        public string DeleteStream => $"{_schema}.delete_stream";
        public string DeleteStreamMessages => $"{_schema}.delete_stream_messages";
        public string ListStreams => $"{_schema}.list_streams";
        public string ListStreamsStartingWith => $"{_schema}.list_streams_starting_with";
        public string ListStreamsEndingWith => $"{_schema}.list_streams_ending_with";
        public string Read => $"{_schema}.read";
        public string ReadAll => $"{_schema}.read_all";
        public string ReadAllHeadPosition => $"{_schema}.read_head_position";
        public string ReadJsonData => $"{_schema}.read_json_data";
        public string ReadSchemaVersion => $"{_schema}.read_schema_version";
        public string ReadStreamMessageBeforeCreatedCount => $"{_schema}.read_stream_message_before_created_count";

        public static string FetchAll(string refcursor) => $@"FETCH ALL IN ""{refcursor}"";";

        public Schema(string schema)
        {
            _schema = schema;
            _scripts = new Scripts(schema);
        }
    }
}