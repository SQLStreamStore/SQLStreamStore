namespace SqlStreamStore.SQLiteScripts
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

        private readonly ConcurrentDictionary<string, string> _scripts =
            new ConcurrentDictionary<string, string>();

        public Scripts(string schema)
        {
            _schema = schema;
        }

        public string Tables => GetScript(nameof(Tables));
        public string Scavenge => GetScript(nameof(Scavenge));  // TODO: create scavange query.

        public string CreateSchema => string.Join(
            Environment.NewLine,
            Tables
        );

        private string GetScript(string name) => _scripts.GetOrAdd(name,
            key =>
            {
                using(var stream = s_assembly.GetManifestResourceStream(typeof(Scripts), $"{key}.sql"))
                {
                    if(stream == null)
                    {
                        throw new Exception($"Embedded resource, {name}, not found. BUG!");
                    }

                    using (StreamReader reader = new StreamReader(stream))
                    {
                        return reader
                            .ReadToEnd()
                            .Replace("__schema__", _schema);
                    }
                }
            });
    }
}