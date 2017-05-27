namespace SqlStreamStore
{
    using System;
    using System.IO;
    using System.Reactive.Linq;
#if NET461
    using System.Runtime.Remoting.Messaging;
#endif
    using Serilog;
    using Serilog.Events;
    using Serilog.Formatting.Display;
    using SqlStreamStore.Infrastructure;
    using Xunit.Abstractions;
#if NETCOREAPP1_0
    using System.Threading;
#endif

    internal static class LoggingHelper
    {
        private const string CaptureCorrelationIdKey = "CaptureCorrelationId";
        private static readonly Subject<LogEvent> s_logEventSubject = new Subject<LogEvent>();

        private static readonly MessageTemplateTextFormatter s_formatter = new MessageTemplateTextFormatter(
            "{Timestamp:yyyy-MM-dd HH:mm:ss.fff zzz} [{Level}] {Message}{NewLine}{Exception}",
            null);

        static LoggingHelper()
        {
            Log.Logger = new LoggerConfiguration()
                .WriteTo
                .Observers(observable => observable.Subscribe(logEvent => s_logEventSubject.OnNext(logEvent)))
                .Enrich.FromLogContext()
                .MinimumLevel.Verbose()
                .CreateLogger();
        }

        public static IDisposable Capture(ITestOutputHelper testOutputHelper)
        {
            var captureId = Guid.NewGuid();

#if NETCOREAPP1_0
            var CallContextData = new AsyncLocal<Tuple<string, Guid>>
            {
                Value = new Tuple<string, Guid>(CaptureCorrelationIdKey, captureId)
            };
#elif NET461
            CallContext.LogicalSetData(CaptureCorrelationIdKey, captureId);
#endif

            Func<LogEvent, bool> filter = logEvent =>
#if NETCOREAPP1_0
                CallContextData.Value.Item2.Equals(captureId);
#elif NET461
                CallContext.LogicalGetData(CaptureCorrelationIdKey).Equals(captureId);
#endif

            var subscription = s_logEventSubject.Where(filter).Subscribe(logEvent =>
            {
                using(var writer = new StringWriter())
                {
                    s_formatter.Format(logEvent, writer);
                    testOutputHelper.WriteLine(writer.ToString());
                }
            });

            return new DisposableAction(() =>
            {
                subscription.Dispose();
#if NETCOREAPP1_0
                CallContextData.Value = null;
#elif NET461
                CallContext.FreeNamedDataSlot(CaptureCorrelationIdKey);
#endif
            });
        }

        private class DisposableAction : IDisposable
        {
            private readonly Action _action;

            public DisposableAction(Action action)
            {
                _action = action;
            }

            public void Dispose()
            {
                _action();
            }
        }
    }
}