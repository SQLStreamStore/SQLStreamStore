namespace Cedar.EventStore.SqlScripts
{
    using System;
    using System.Collections.Concurrent;
    using System.IO;

    internal class Scripts
    {
        internal readonly string Schema;
        private readonly ConcurrentDictionary<string, string> _scripts 
            = new ConcurrentDictionary<string, string>();

        internal Scripts(string schema)
        {
            Schema = schema;
        }

        internal string AppendStreamExpectedVersionAny => GetScript(nameof(AppendStreamExpectedVersionAny));

        internal string AppendStreamExpectedVersion => GetScript(nameof(AppendStreamExpectedVersion));

        internal string AppendStreamExpectedVersionNoStream => GetScript(nameof(AppendStreamExpectedVersionNoStream));

        internal string DeleteStreamAnyVersion => GetScript(nameof(DeleteStreamAnyVersion));

        internal string DeleteStreamEvent => GetScript(nameof(DeleteStreamEvent));

        internal string DeleteStreamExpectedVersion => GetScript(nameof(DeleteStreamExpectedVersion));

        internal string DropAll => GetScript(nameof(DropAll));

        internal string GetStreamEventCount => GetScript(nameof(GetStreamEventCount));

        internal string GetStreamEventBeforeCreatedCount => GetScript(nameof(GetStreamEventBeforeCreatedCount));

        internal string InitializeStore => GetScript(nameof(InitializeStore));

        internal string ReadAllForward => GetScript(nameof(ReadAllForward));

        internal string ReadHeadCheckpoint => GetScript(nameof(ReadHeadCheckpoint));

        internal string ReadAllBackward => GetScript(nameof(ReadAllBackward));

        internal string ReadStreamForward => GetScript(nameof(ReadStreamForward));

        internal string ReadStreamBackward => GetScript(nameof(ReadStreamBackward));

        internal string SetStreamMetadata => GetScript(nameof(SetStreamMetadata));

        private string GetScript(string name)
        {
            return _scripts.GetOrAdd(name,
                key =>
                {
                    using(Stream stream = typeof(Scripts)
                        .Assembly
                        .GetManifestResourceStream("Cedar.EventStore.MsSqlScripts." + key + ".sql"))
                    {
                        if(stream == null)
                        {
                            throw new Exception("Embedded resource not found. BUG!");
                        }
                        using(StreamReader reader = new StreamReader(stream))
                        {
                            return reader
                                .ReadToEnd()
                                .Replace("dbo.", Schema + ".");
                        }
                    }
                });
        }
    }
}