namespace SqlStreamStore.MySql
{
    using System;
    using System.Collections.Generic;
    using MySqlConnector.Logging;
    using SqlStreamStore.Logging;

    public class LibLogMySqlConnectorLoggerProvider : IMySqlConnectorLoggerProvider
    {
        public IMySqlConnectorLogger CreateLogger(string name)
            => new LibLogMySqlConnectorLogger(LogProvider.GetLogger(name));

        private class LibLogMySqlConnectorLogger : IMySqlConnectorLogger
        {
            private static readonly IDictionary<MySqlConnectorLogLevel, Func<ILog, bool>>
                s_isEnabledByLevel = new Dictionary<MySqlConnectorLogLevel, Func<ILog, bool>>
                {
                    [MySqlConnectorLogLevel.Trace] = LogExtensions.IsTraceEnabled,
                    [MySqlConnectorLogLevel.Debug] = LogExtensions.IsDebugEnabled,
                    [MySqlConnectorLogLevel.Info] = LogExtensions.IsInfoEnabled,
                    [MySqlConnectorLogLevel.Warn] = LogExtensions.IsWarnEnabled,
                    [MySqlConnectorLogLevel.Error] = LogExtensions.IsErrorEnabled,
                    [MySqlConnectorLogLevel.Fatal] = LogExtensions.IsFatalEnabled
                };

            private static readonly IDictionary<MySqlConnectorLogLevel, Action<ILog, Exception, string, object[]>>
                s_logByLevel = new Dictionary<MySqlConnectorLogLevel, Action<ILog, Exception, string, object[]>>
                {
                    [MySqlConnectorLogLevel.Trace] = LogExtensions.Trace,
                    [MySqlConnectorLogLevel.Debug] = LogExtensions.Debug,
                    [MySqlConnectorLogLevel.Info] = LogExtensions.Info,
                    [MySqlConnectorLogLevel.Warn] = LogExtensions.Warn,
                    [MySqlConnectorLogLevel.Error] = LogExtensions.Error,
                    [MySqlConnectorLogLevel.Fatal] = LogExtensions.Fatal
                };

            private readonly ILog _logger;

            public LibLogMySqlConnectorLogger(ILog logger)
            {
                _logger = logger;
            }

            public bool IsEnabled(MySqlConnectorLogLevel level) => s_isEnabledByLevel[level](_logger);

            public void Log(MySqlConnectorLogLevel level, string msg, object[] args, Exception exception = null)
                => s_logByLevel[level](_logger, exception, msg, args);
        }
    }
}