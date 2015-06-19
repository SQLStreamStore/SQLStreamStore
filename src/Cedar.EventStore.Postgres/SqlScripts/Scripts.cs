namespace Cedar.EventStore.Postgres.SqlScripts
{
    using System;
    using System.Collections.Concurrent;
    using System.IO;

    public static class Scripts
    {
        private static readonly ConcurrentDictionary<string, string> s_scripts 
            = new ConcurrentDictionary<string, string>(); 

        public static string InitializeStore
        {
            get { return GetScript("InitializeStore"); }
        }

        public static string DropAll
        {
            get { return GetScript("DropAll"); }
        }

        public static string BulkCopyEvents
        {
            get { return GetScript("BulkCopyEvents"); }
        }

        public static string ReadAllForward
        {
            get { return GetScript("ReadAllForward"); }
        }

        public static string ReadAllBackward
        {
            get { return GetScript("ReadAllBackward"); }
        }

        private static string GetScript(string name)
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

        public static class Functions
        {
            public static string CreateStream
            {
                get { return "create_stream"; }
            }

            public static string GetStream
            {
                get { return "get_stream"; }
            }

            public static string ReadAllForward
            {
                get { return "read_all_forward"; }
            }

            public static string ReadAllBackward
            {
                get { return "read_all_backward"; }
            }

            public static string ReadStreamForward
            {
                get { return "read_stream_forward"; }
            }

            public static string ReadStreamBackward
            {
                get { return "read_stream_backward"; }
            }

            public static string DeleteStreamAnyVersion
            {
                get { return "delete_stream_any_version"; }
            }

            public static string DeleteStreamExpectedVersion
            {
                get { return "delete_stream_expected_version"; }
            }
        }
    }
}