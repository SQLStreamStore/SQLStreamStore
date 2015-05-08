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
            get { return s_scripts.GetOrAdd("InitializeStore", ReadResource); }
        }

        public static string DropAll
        {
            get { return s_scripts.GetOrAdd("DropAll", ReadResource); }
        }

        private static string ReadResource(string name)
        {
            using(Stream stream = typeof(Scripts)
                .Assembly
                .GetManifestResourceStream("Cedar.EventStore.SqlScripts." + name + ".sql"))
            {
                using (StreamReader reader = new StreamReader(stream))
                {
                    return reader.ReadToEnd();
                }
            }
        }
    }
}