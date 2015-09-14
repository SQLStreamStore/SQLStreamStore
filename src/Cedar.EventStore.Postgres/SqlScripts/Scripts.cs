namespace Cedar.EventStore.Postgres.SqlScripts
{
    using System;
    using System.Collections.Concurrent;
    using System.IO;

    public class Scripts
    {
        private static readonly ConcurrentDictionary<string, string> s_scripts
            = new ConcurrentDictionary<string, string>();

        private readonly string _schema;

        public Scripts(string schema = "public")
        {
            _schema = schema;
            Functions = new GetFunctions(_schema);
        }

        public string InitializeStore
        {
            get { return GetScript("InitializeStore").Replace("$schema$", _schema); }
        }

        public string DropAll
        {
            get { return GetScript("DropAll").Replace("$schema$", _schema); }
        }

        public string BulkCopyEvents
        {
            get { return GetScript("BulkCopyEvents").Replace("$schema$", _schema); }
        }

        public string ReadAllForward
        {
            get { return GetScript("ReadAllForward").Replace("$schema$", _schema); }
        }

        public string ReadAllBackward
        {
            get { return GetScript("ReadAllBackward").Replace("$schema$", _schema); }
        }

        public GetFunctions Functions { get; private set; }

        private string GetScript(string name)
        {
            return s_scripts.GetOrAdd(name,
                key =>
                {
                    using(Stream stream = typeof(Scripts)
                        .Assembly
                        .GetManifestResourceStream("Cedar.EventStore.Postgres.SqlScripts." + key + ".sql"))
                    {
                        if(stream == null)
                        {
                            throw new Exception("Embedded resource not found. BUG!");
                        }
                        using(StreamReader reader = new StreamReader(stream))
                        {
                            return reader.ReadToEnd();
                        }
                    }
                });
        }

        public class GetFunctions
        {
            private readonly string _schema;

            public GetFunctions(string schema)
            {
                _schema = schema;
            }

            public string CreateStream
            {
                get { return string.Concat(_schema, ".", "create_stream"); }
            }

            public string GetStream
            {
                get { return string.Concat(_schema, ".", "get_stream"); }
            }

            public string ReadAllForward
            {
                get { return string.Concat(_schema, ".", "read_all_forward"); }
            }

            public string ReadAllBackward
            {
                get { return string.Concat(_schema, ".", "read_all_backward"); }
            }

            public string ReadStreamForward
            {
                get { return string.Concat(_schema, ".", "read_stream_forward"); }
            }

            public string ReadStreamBackward
            {
                get { return string.Concat(_schema, ".", "read_stream_backward"); }
            }

            public string DeleteStreamAnyVersion
            {
                get { return string.Concat(_schema, ".", "delete_stream_any_version"); }
            }

            public string DeleteStreamExpectedVersion
            {
                get { return string.Concat(_schema, ".", "delete_stream_expected_version"); }
            }
        }
    }
}