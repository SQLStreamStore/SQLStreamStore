namespace Cedar.EventStore.SqlScripts
{
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

        private static string GetScript(string name)
        {
            return s_scripts.GetOrAdd(name,
                key =>
                {
                    using(Stream stream = typeof(Scripts)
                        .Assembly
                        .GetManifestResourceStream("Cedar.EventStore.SqlScripts." + key + ".sql"))
                    {
                        using(StreamReader reader = new StreamReader(stream))
                        {
                            return reader.ReadToEnd();
                        }
                    }
                });
        }
    }
}