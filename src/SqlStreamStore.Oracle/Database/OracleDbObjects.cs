namespace SqlStreamStore.OracleDatabase
{
    public class OracleDbObjects
    {
        private readonly string _schemaAndPrefix;

        public OracleDbObjects(string schema = "public")
        {
            var package = "StreamStore.";
            _schemaAndPrefix = $"{schema}.";

            Append = $"{package}APPEND";
            ReadStream = $"{package}READ";
            ReadAll = $"{package}READALL";
            DeleteStreamExpectedVersion = $"{package}DELETESTREAM_EXPECTEDVERSION";
            DeleteStreamAnyVersion = $"{package}DELETESTREAM_ANYVERSION";
            SetStreamMeta = $"{package}SETMETA";
            
            TableStreamEvents = $"{_schemaAndPrefix}STREAMEVENTS";
            TableStreams = $"{_schemaAndPrefix}STREAMS";
        }

        public string Append { get; }
        public string ReadStream { get; }
        public string ReadAll { get; }
        
        public string DeleteStreamExpectedVersion { get; }
        public string DeleteStreamAnyVersion { get; }
        
        public string SetStreamMeta { get; set; }
        
        public string TableStreamEvents { get; }
        public string TableStreams { get; }
    }
}