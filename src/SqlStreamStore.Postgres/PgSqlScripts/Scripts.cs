namespace SqlStreamStore.PgSqlScripts
{
    using System;
    using System.Collections.Concurrent;
    using System.IO;
    using System.Reflection;

    internal class Scripts
    {
        private readonly string _schema;

        private readonly ConcurrentDictionary<string, string> _scripts
            = new ConcurrentDictionary<string, string>();

        private static readonly Assembly s_assembly = typeof(Scripts)
            .GetTypeInfo()
            .Assembly;

        public Scripts(string schema)
        {
            _schema = schema;
        }

        public string DropAll => GetScript(nameof(DropAll));

        private string Tables => GetScript(nameof(Tables));

        private string AppendToStream => GetScript(nameof(AppendToStream));

        private string DeleteStream => GetScript(nameof(DeleteStream));
        private string DeleteStreamMessage => GetScript(nameof(DeleteStreamMessage));

        private string ReadAll => GetScript(nameof(ReadAll));
        private string Read => GetScript(nameof(Read));
        private string ReadJsonData => GetScript(nameof(ReadJsonData));
        private string ReadHeadPosition => GetScript(nameof(ReadHeadPosition));
        private string ReadStreamMessageCount => GetScript(nameof(ReadStreamMessageCount));
        private string ReadStreamMessageBeforeCreatedCount => GetScript(nameof(ReadStreamMessageBeforeCreatedCount));
        private string ReadStreamVersionOfMessageId => GetScript(nameof(ReadStreamVersionOfMessageId));

        public string CreateSchema => string.Join(
            Environment.NewLine,
            Tables,
            AppendToStream,
            DeleteStream,
            DeleteStreamMessage,
            Read,
            ReadAll,
            ReadJsonData,
            ReadHeadPosition,
            ReadStreamMessageCount,
            ReadStreamMessageBeforeCreatedCount,
            ReadStreamVersionOfMessageId);

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
                            .Replace("public.", _schema + ".");
                    }
                }
            });
    }
}