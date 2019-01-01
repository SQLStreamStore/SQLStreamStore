namespace SqlStreamStore.HAL
{
    using System;
    using System.IO;
    using System.Threading;
    using System.Threading.Tasks;
    using Microsoft.AspNetCore.Builder;
    using Microsoft.AspNetCore.Http;
    using SqlStreamStore.HAL.AllStream;
    using SqlStreamStore.HAL.AllStreamMessage;
    using SqlStreamStore.HAL.Docs;
    using SqlStreamStore.HAL.Index;
    using SqlStreamStore.HAL.Logging;
    using SqlStreamStore.HAL.StreamBrowser;
    using SqlStreamStore.HAL.StreamMessage;
    using SqlStreamStore.HAL.StreamMetadata;
    using SqlStreamStore.HAL.Streams;
    using MidFunc = System.Func<
        Microsoft.AspNetCore.Http.HttpContext,
        System.Func<System.Threading.Tasks.Task>,
        System.Threading.Tasks.Task
    >;

    public static class SqlStreamStoreHalMiddleware
    {
        private static ILog s_Log = LogProvider.GetLogger(typeof(SqlStreamStoreHalMiddleware));

        private static MidFunc CaseSensitiveQueryStrings => (context, next) =>
        {
            if(context.Request.QueryString != QueryString.Empty)
            {
                var queryString = context.Request.QueryString;
                context.Request.Query = new CaseSensitiveQueryCollection(queryString);
                // Setting context.Request.Query mutates context.Request.QueryString.
                // This has the unfortunate side effect of turning ?a=1&b into ?a=1&b=.
                // so, replace with original context.Request.QueryString and call it a day.
                context.Request.QueryString = queryString;
            }

            return next();
        };

        private static MidFunc HeadRequests => async (context, next) =>
        {
            using(new OptionalHeadRequestWrapper(context))
            {
                await next();
            }
        };

        public static IApplicationBuilder UseSqlStreamStoreHal(
            this IApplicationBuilder builder,
            IStreamStore streamStore,
            SqlStreamStoreMiddlewareOptions options = default)
        {
            if(builder == null)
                throw new ArgumentNullException(nameof(builder));
            if(streamStore == null)
                throw new ArgumentNullException(nameof(streamStore));

            options = options ?? new SqlStreamStoreMiddlewareOptions();

            var index = new IndexResource(streamStore);
            var allStream = new AllStreamResource(streamStore, options.UseCanonicalUrls);
            var allStreamMessages = new AllStreamMessageResource(streamStore);
            var streamBrowser = new StreamBrowserResource(streamStore);
            var streams = new StreamResource(streamStore);
            var streamMetadata = new StreamMetadataResource(streamStore);
            var streamMessages = new StreamMessageResource(streamStore);
            var documentation = new DocsResource(
                index,
                allStream,
                allStreamMessages,
                streams,
                streamMessages,
                streamMetadata);

            s_Log.Info(index.ToString);

            return builder
                .UseExceptionHandling()
                .Use(CaseSensitiveQueryStrings)
                .Use(HeadRequests)
                .UseDocs(documentation)
                .UseIndex(index)
                .UseAllStream(allStream)
                .UseAllStreamMessage(allStreamMessages)
                .UseStreamBrowser(streamBrowser)
                .UseStreams(streams)
                .UseStreamMetadata(streamMetadata)
                .UseStreamMessages(streamMessages);
        }

        private class OptionalHeadRequestWrapper : IDisposable
        {
            private readonly HttpContext _context;
            private readonly Stream _originalBody;
            private readonly bool _isHeadRequest;

            public OptionalHeadRequestWrapper(HttpContext context)
            {
                _context = context;
                if(_context.Request.Method != "HEAD")
                {
                    return;
                }

                _isHeadRequest = true;
                _originalBody = _context.Response.Body;
                _context.Request.Method = "GET";
                _context.Response.Body = new HeadRequestStream();
            }

            public void Dispose()
            {
                if(!_isHeadRequest)
                {
                    return;
                }

                _context.Response.Body = _originalBody;
                _context.Request.Method = "HEAD";
            }

            private class HeadRequestStream : Stream
            {
                private long _length;

                public override void Flush() => FlushAsync(CancellationToken.None).Wait();
                public override int Read(byte[] buffer, int offset, int count) => throw new NotImplementedException();
                public override long Seek(long offset, SeekOrigin origin) => throw new NotImplementedException();
                public override void SetLength(long value) => throw new NotImplementedException();
                public override void Write(byte[] buffer, int offset, int count) => throw new NotImplementedException();

                public override Task WriteAsync(
                    byte[] buffer,
                    int offset,
                    int count,
                    CancellationToken cancellationToken)
                {
                    _length += count;
                    Position += count;
                    return Task.CompletedTask;
                }

                public override Task FlushAsync(CancellationToken cancellationToken) => Task.CompletedTask;

                public override bool CanRead { get; } = false;
                public override bool CanSeek { get; } = false;
                public override bool CanWrite { get; } = true;
                public override long Length => _length;
                public override long Position { get; set; }
            }
        }
    }
}