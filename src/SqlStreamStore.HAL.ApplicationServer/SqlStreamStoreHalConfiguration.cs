namespace SqlStreamStore.HAL.ApplicationServer
{
    using System;
    using System.Collections;
    using System.Linq;
    using Microsoft.Extensions.Configuration;
    using Serilog.Events;

    internal class SqlStreamStoreHalConfiguration
    {
        private readonly IConfigurationRoot _configuration;

        public bool UseCanonicalUris => _configuration.GetValue<bool>("use-canonical-uris");
        public LogEventLevel LogLevel => _configuration.GetValue("log-level", LogEventLevel.Information);
        public string ConnectionString => _configuration.GetValue<string>("connection-string");
        public string Schema => _configuration.GetValue<string>("schema");
        public string Provider => _configuration.GetValue<string>("provider");

        public SqlStreamStoreHalConfiguration(IDictionary environment, string[] args)
        {
            if(environment == null)
                throw new ArgumentNullException(nameof(environment));
            if(args == null)
                throw new ArgumentNullException(nameof(args));
            _configuration = new ConfigurationBuilder()
                .AddCommandLine(args)
                .Add(new UpperCasedEnvironmentVariablesConfigurationSource(environment))
                .Build();
        }

        private class UpperCasedEnvironmentVariablesConfigurationSource : IConfigurationSource
        {
            private readonly IDictionary _environment;
            public string Prefix { get; set; } = "SQLSTREAMSTORE";

            public UpperCasedEnvironmentVariablesConfigurationSource(IDictionary environment)
            {
                _environment = environment;
            }

            public IConfigurationProvider Build(IConfigurationBuilder builder)
                => new UpperCasedEnvironmentVariablesConfigurationProvider(Prefix, _environment);
        }

        private class UpperCasedEnvironmentVariablesConfigurationProvider : ConfigurationProvider
        {
            private readonly IDictionary _environment;
            private readonly string _prefix;

            public UpperCasedEnvironmentVariablesConfigurationProvider(string prefix, IDictionary environment)
            {
                _environment = environment;
                _prefix = $"{prefix}_";
            }

            public override void Load()
            {
                Data = (from entry in _environment.OfType<DictionaryEntry>()
                        let key = (string) entry.Key
                        where key.StartsWith(_prefix)
                        select new { key = key.Remove(0, _prefix.Length).Replace('_', '-').ToLowerInvariant(), value = (string) entry.Value })
                    .ToDictionary(x => x.key, x => x.value);
            }
        }
    }
}
