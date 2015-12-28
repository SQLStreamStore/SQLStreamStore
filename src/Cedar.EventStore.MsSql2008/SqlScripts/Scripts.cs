namespace Cedar.EventStore.SqlScripts
{
    using System;
    using System.Collections.Concurrent;
    using System.IO;

    internal static class Scripts
    {
        private static readonly ConcurrentDictionary<string, string> s_scripts 
            = new ConcurrentDictionary<string, string>();

        internal static string AppendStreamExpectedVersionAny => GetScript("AppendStreamExpectedVersionAny");

        internal static string AppendStreamExpectedVersion => GetScript("AppendStreamExpectedVersion");

        internal static string AppendStreamExpectedVersionNoStream => GetScript("AppendStreamExpectedVersionNoStream");

        internal static string DeleteStreamAnyVersion => GetScript("DeleteStreamAnyVersion");

        internal static string DeleteStreamExpectedVersion => GetScript("DeleteStreamExpectedVersion");

        internal static string DropAll => GetScript("DropAll");

        internal static string InitializeStore => GetScript("InitializeStore");

        internal static string ReadAllForward => GetScript("ReadAllForward");

        internal static string ReadAllBackward => GetScript("ReadAllBackward");

        internal static string ReadStreamForward => GetScript("ReadStreamForward");

        internal static string ReadStreamBackward => GetScript("ReadStreamBackward");

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