namespace SqlStreamStore.OracleScripts
{
    using System;
    using System.IO;
    using System.Reflection;

    public class Scripts
    {
        private static readonly Assembly s_scriptAssembly = typeof(Scripts).GetTypeInfo().Assembly;
        private readonly string _schemaAndPrefix;

        public Scripts(string schema = "public", string prefix = null)
        {
            _schemaAndPrefix = $"{schema}.{prefix}";

            AppendToStream = new Lazy<string>(() => Build(nameof(AppendToStream)));
        }

        private string Build(string script)
        {
            using(var stream = s_scriptAssembly.GetManifestResourceStream(typeof(Scripts), $"{script}.sql"))
            {
                if(stream == null)
                {
                    throw new Exception($"Embedded resource, {script}, not found. BUG!");
                }

                using(StreamReader reader = new StreamReader(stream))
                {
                    return reader
                        .ReadToEnd()
                        .Replace("__schema__", _schemaAndPrefix);
                }
            }
        }
        
        public Lazy<string> AppendToStream { get; }
    }
}