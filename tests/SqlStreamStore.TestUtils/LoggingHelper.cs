namespace SqlStreamStore.TestUtils
{
    using System;
    using System.IO;
    using System.Reactive.Linq;
    using System.Threading;
    using Serilog;
    using Serilog.Events;
    using Serilog.Formatting.Display;
    using SqlStreamStore.V1.Infrastructure;
    using Xunit.Abstractions;

    public static class LoggingHelper
    {
        private const string CaptureCorrelationIdKey = "CaptureCorrelationId";
        private static readonly Subject<LogEvent> s_logEventSubject = new Subject<LogEvent>();

        private static readonly MessageTemplateTextFormatter s_formatter = new MessageTemplateTextFormatter(
            "{Timestamp:yyyy-MM-dd HH:mm:ss.fff zzz} [{Level}] {Message}",
            null);

        private static readonly MessageTemplateTextFormatter s_formatterWithException = new MessageTemplateTextFormatter(
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

            var callContextData = new AsyncLocal<Tuple<string, Guid>>
            {
                Value = new Tuple<string, Guid>(CaptureCorrelationIdKey, captureId)
            };

            bool Filter(LogEvent logEvent) => callContextData.Value.Item2.Equals(captureId);

            var subscription = s_logEventSubject.Where(Filter).Subscribe(logEvent =>
            {
                using (var writer = new StringWriter())
                {
                    if(logEvent.Exception != null)
                    {
                        s_formatterWithException.Format(logEvent, writer);
                    }
                    else
                    {
                        s_formatter.Format(logEvent, writer);
                    }
                    testOutputHelper.WriteLine(writer.ToString());
                }
            });

            return new DisposableAction(() =>
            {
                subscription.Dispose();
                callContextData.Value = null;
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