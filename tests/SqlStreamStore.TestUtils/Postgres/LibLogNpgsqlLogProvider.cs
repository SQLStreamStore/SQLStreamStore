namespace SqlStreamStore.Postgres
{
    using System;
    using Npgsql.Logging;
    using SqlStreamStore.Logging;

    public class LibLogNpgsqlLogProvider : INpgsqlLoggingProvider
    {
        public NpgsqlLogger CreateLogger(string name)
        {
            var logger = LogProvider.GetLogger(name);
            return new LibLogNpgsqlLogger(logger, name);
        }

        private class LibLogNpgsqlLogger : NpgsqlLogger
        {
            private readonly ILog _logger;
            private readonly string _name;

            public LibLogNpgsqlLogger(ILog logger, string name)
            {
                _logger = logger;
                _name = name;
            }

            public override bool IsEnabled(NpgsqlLogLevel level) => true;

            public override void Log(NpgsqlLogLevel level, int connectorId, string msg, Exception exception = null)
                => _logger.Info($@"[{level:G}] [{_name}] (Connector Id: {connectorId}); {msg}; {FormatOptionalException(exception)}");

            private static string FormatOptionalException(Exception exception)
                => exception == null ? string.Empty : $"(Exception: {exception})";
        }
    }
}