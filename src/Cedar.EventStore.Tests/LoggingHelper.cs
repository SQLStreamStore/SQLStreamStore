namespace Cedar.EventStore
{
    using System;
    using System.IO;
    using System.Reactive.Linq;
    using System.Runtime.Remoting.Messaging;
    using Cedar.EventStore.Infrastructure;
    using Serilog;
    using Serilog.Events;
    using Serilog.Formatting.Display;
    using Xunit.Abstractions;

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
                .CreateLogger();
        }

        public static IDisposable Capture(ITestOutputHelper testOutputHelper)
        {
            var captureId = Guid.NewGuid();

            CallContext.LogicalSetData(CaptureCorrelationIdKey, captureId);

            Func<LogEvent, bool> filter = logEvent =>
                CallContext.LogicalGetData(CaptureCorrelationIdKey).Equals(captureId);

            var subscription = s_logEventSubject.Where(filter).Subscribe(logEvent =>
            {
                using (var writer = new StringWriter())
                {
                    s_formatter.Format(logEvent, writer);
                    testOutputHelper.WriteLine(writer.ToString());
                }
            });

            return new DisposableAction(() =>
            {
                subscription.Dispose();
                CallContext.FreeNamedDataSlot(CaptureCorrelationIdKey);
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