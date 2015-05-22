namespace Cedar.EventStore.SqlScripts
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

        public static string CreateStream
        {
            get { return GetScript("CreateStream"); }
        }

        public static string DeleteStreamAnyVersion
        {
            get { return GetScript("DeleteStreamAnyVersion"); }
        }

        public static string DeleteStreamExpectedVersion
        {
            get { return GetScript("DeleteStreamExpectedVersion"); }
        }

        public static string ReadAllForward
        {
            get { return GetScript("ReadAllForward"); }
        }

        public static string ReadAllBackward
        {
            get { return GetScript("ReadAllBackward"); }
        }

        public static string ReadStreamForward
        {
            get { return GetScript("ReadStreamForward"); }
        }

        public static string ReadStreamBackward
        {
            get { return GetScript("ReadStreamBackward"); }
        }

        private static string GetScript(string name)
        {
            return s_scripts.GetOrAdd(name,
                key =>
                {
                    using(Stream stream = typeof(Scripts)
                        .Assembly
                        .GetManifestResourceStream("Cedar.EventStore.SqlScripts." + key + ".sql"))
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
    }
}