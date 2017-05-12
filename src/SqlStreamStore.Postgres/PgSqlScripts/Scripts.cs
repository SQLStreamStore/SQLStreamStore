namespace SqlStreamStore.PgSqlScripts
{
    using System;
    using System.Collections.Concurrent;
    using System.IO;
    using System.Reflection;

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

        internal string DeleteStreamMessage => GetScript(nameof(DeleteStreamMessage));

        internal string DeleteStreamExpectedVersion => GetScript(nameof(DeleteStreamExpectedVersion));

        internal string DropAll => GetScript(nameof(DropAll));

        internal string GetStreamMessageCount => GetScript(nameof(GetStreamMessageCount));

        internal string GetStreamMessageBeforeCreatedCount => GetScript(nameof(GetStreamMessageBeforeCreatedCount));

        internal string CreateSchema => GetScript(nameof(CreateSchema));

        internal string ReadAllForward => GetScript(nameof(ReadAllForward));

        internal string ReadHeadPosition => GetScript(nameof(ReadHeadPosition));

        internal string ReadAllBackward => GetScript(nameof(ReadAllBackward));

        internal string ReadStreamForward => GetScript(nameof(ReadStreamForward));

        internal string ReadStreamBackward => GetScript(nameof(ReadStreamBackward));

        internal string SetStreamMetadata => GetScript(nameof(SetStreamMetadata));

        private string GetScript(string name)
        {
            return _scripts.GetOrAdd(name,
                key =>
                {
#if NETSTANDARD1_3
                    using (Stream stream = typeof(Scripts).GetTypeInfo().Assembly.GetManifestResourceStream("SqlStreamStore.Postgres.PgSqlScripts." + key + ".pgsql"))
#elif NET46
                    using (Stream stream = typeof(Scripts).Assembly.GetManifestResourceStream("SqlStreamStore.Postgres.PgSqlScripts." + key + ".pgsql"))
#endif
                    {
                        if (stream == null)
                        {
                            throw new Exception($"Embedded resource, {name}, not found. BUG!");
                        }
                        using (StreamReader reader = new StreamReader(stream))
                        {
                            return reader
                                .ReadToEnd()
                                .Replace("public.", Schema + ".");
                        }
                    }
                });
        }
    }
}