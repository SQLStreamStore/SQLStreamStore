namespace SqlStreamStore.PgSqlScriptsV1
{
    using System;
    using System.Collections.Concurrent;
    using System.IO;
    using System.Reflection;

    internal class Scripts
    {
        private static readonly Assembly s_assembly = typeof(Scripts)
            .GetTypeInfo()
            .Assembly;

        private readonly string _schema;

        private readonly ConcurrentDictionary<string, string> _scripts
            = new ConcurrentDictionary<string, string>();

        public Scripts(string schema)
        {
            _schema = schema;
        }

        public string DropAll => GetScript(nameof(DropAll));

        public string EnableExplainAnalyze => GetScript(nameof(EnableExplainAnalyze));

        private string Tables => GetScript(nameof(Tables));

        private string AppendToStream => GetScript(nameof(AppendToStream));
        private string DeleteStream => GetScript(nameof(DeleteStream));

        private string DeleteStreamMessages => GetScript(nameof(DeleteStreamMessages));
        private string EnforceIdempotentAppend => GetScript(nameof(EnforceIdempotentAppend));
        private string ListStreams => GetScript(nameof(ListStreams));
        private string ListStreamsStartingWith => GetScript(nameof(ListStreamsStartingWith));
        private string ListStreamsEndingWith => GetScript(nameof(ListStreamsEndingWith));
        private string ReadAll => GetScript(nameof(ReadAll));

        private string Read => GetScript(nameof(Read));

        private string ReadJsonData => GetScript(nameof(ReadJsonData));

        private string ReadHeadPosition => GetScript(nameof(ReadHeadPosition));

        private string ReadStreamHeadPosition => GetScript(nameof(ReadStreamHeadPosition));

        private string ReadStreamHeadVersion => GetScript(nameof(ReadStreamHeadVersion));

        private string ReadSchemaVersion => GetScript(nameof(ReadSchemaVersion));

        private string ReadStreamVersionOfMessageId => GetScript(nameof(ReadStreamVersionOfMessageId));

        private string Scavenge => GetScript(nameof(Scavenge));

        private string SetStreamMetadata => GetScript(nameof(SetStreamMetadata));


        public string CreateSchema => string.Join(
            Environment.NewLine,
            Tables,
            AppendToStream,
            DeleteStream,
            DeleteStreamMessages,
            EnforceIdempotentAppend,
            ListStreams,
            ListStreamsStartingWith,
            ListStreamsEndingWith,
            Read,
            ReadAll,
            ReadJsonData,
            ReadHeadPosition,
            ReadStreamHeadPosition,
            ReadStreamHeadVersion,
            ReadSchemaVersion,
            ReadStreamVersionOfMessageId,
            Scavenge,
            SetStreamMetadata);

        private string GetScript(string name) => _scripts.GetOrAdd(name,
            key =>
            {
                using(var stream = s_assembly.GetManifestResourceStream(typeof(Scripts), $"{key}.sql"))
                {
                    if(stream == null)
                    {
                        throw new Exception($"Embedded resource, {name}, not found. BUG!");
                    }

                    using(StreamReader reader = new StreamReader(stream))
                    {
                        return reader
                            .ReadToEnd()
                            .Replace("__schema__", _schema);
                    }
                }
            });
    }
}