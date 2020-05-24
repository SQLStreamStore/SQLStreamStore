namespace SqlStreamStore.OracleDatabase
{
    public class OracleDbObjects
    {
        private readonly string _schemaAndPrefix;

        public OracleDbObjects(string schema = "public")
        {
            var package = "StreamStore.";
            _schemaAndPrefix = $"{schema}.";

            AppendToStreamExpectedVersion = $"{package}STREAM_APPEND_EXPECTEDVERSION";
            AppendToStreamNoStream = $"{package}STREAM_APPEND_NOSTREAM";
            AppendToStreamAnyVersion = $"{package}STREAM_APPEND_ANYVERSION";
            ReadStream = $"{package}STREAM_READ";
            ReadAll = $"{package}STREAM_READALL";
            DeleteStreamExpectedVersion = $"{package}STREAM_DELETESTREAM_EXPECTEDVERSION";
            DeleteStreamAnyVersion = $"{package}STREAM_DELETESTREAM_ANYVERSION";
            SetStreamMeta = $"{package}STREAM_SETMETA";
            
            TableStreamEvents = $"{_schemaAndPrefix}STREAMEVENTS";
            TableStreams = $"{_schemaAndPrefix}STREAMS";
        }

        public string AppendToStreamExpectedVersion { get; }
        
        public string AppendToStreamNoStream { get; }
        
        public string AppendToStreamAnyVersion { get; }
        
        public string ReadStream { get; }
        public string ReadAll { get; }
        
        public string DeleteStreamExpectedVersion { get; }
        public string DeleteStreamAnyVersion { get; }
        
        public string SetStreamMeta { get; set; }
        
        public string TableStreamEvents { get; }
        public string TableStreams { get; }
    }
}