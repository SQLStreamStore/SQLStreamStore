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

        public string Tables => GetScript(nameof(Tables));

        public string ReadAll => GetScript(nameof(ReadAll));

        public string Read => GetScript(nameof(Read));

        public string ReadJsonData => GetScript(nameof(ReadJsonData));

        public string AppendToStream => GetScript(nameof(AppendToStream));

        public string CreateSchema => string.Join(
            Environment.NewLine,
            Tables,
            Read,
            ReadAll,
            ReadJsonData,
            AppendToStream);

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