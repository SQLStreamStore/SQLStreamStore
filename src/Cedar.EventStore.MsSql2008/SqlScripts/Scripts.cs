namespace Cedar.EventStore.SqlScripts
{
    using System;
    using System.Collections.Concurrent;
    using System.IO;

    internal static class Scripts
    {
        private static readonly ConcurrentDictionary<string, string> s_scripts 
            = new ConcurrentDictionary<string, string>();

        internal static string AppendStreamExpectedVersionAny => GetScript(nameof(AppendStreamExpectedVersionAny));

        internal static string AppendStreamExpectedVersion => GetScript(nameof(AppendStreamExpectedVersion));

        internal static string AppendStreamExpectedVersionNoStream => GetScript(nameof(AppendStreamExpectedVersionNoStream));

        internal static string DeleteStreamAnyVersion => GetScript(nameof(DeleteStreamAnyVersion));

        internal static string DeleteStreamExpectedVersion => GetScript(nameof(DeleteStreamExpectedVersion));

        internal static string DropAll => GetScript(nameof(DropAll));

        internal static string InitializeStore => GetScript(nameof(InitializeStore));

        internal static string ReadAllForward => GetScript(nameof(ReadAllForward));

        internal static string ReadHeadCheckpoint => GetScript(nameof(ReadHeadCheckpoint));

        internal static string ReadAllBackward => GetScript(nameof(ReadAllBackward));

        internal static string ReadStreamForward => GetScript(nameof(ReadStreamForward));

        internal static string ReadStreamBackward => GetScript(nameof(ReadStreamBackward));

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