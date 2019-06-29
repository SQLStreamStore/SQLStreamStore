namespace SqlStreamStore
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Net;
    using System.Threading;
    using System.Threading.Tasks;
    using SqlStreamStore.Imports.Ensure.That;
    using SqlStreamStore.Internal.HoneyBearHalClient;
    using SqlStreamStore.Internal.HoneyBearHalClient.Models;
    using SqlStreamStore.Streams;

    partial class HttpClientSqlStreamStore
    {
        public ReadStreamResult ReadStreamForwards(
            StreamId streamId,
            int fromVersionInclusive,
            int maxCount,
            bool prefetchJsonData = true,
            CancellationToken cancellationToken = default)
        {
            Ensure.That(fromVersionInclusive, nameof(fromVersionInclusive)).IsGte(0);
            Ensure.That(maxCount, nameof(maxCount)).IsGte(1);

            GuardAgainstDisposed();

            var client = CreateClient();

            var reader = new StreamForwardsReader(
                client,
                streamId,
                fromVersionInclusive,
                maxCount,
                prefetchJsonData);

            IAsyncEnumerable<StreamMessage> NoOp(StreamMessage[] messages, CancellationToken ct)
                => messages.ToAsyncEnumerable();

            return new ReadStreamResult(reader.Read, NoOp, cancellationToken);
        }

        public ReadStreamResult ReadStreamBackwards(
            StreamId streamId,
            int fromVersionInclusive,
            int maxCount,
            bool prefetchJsonData = true,
            CancellationToken cancellationToken = default)
        {
            Ensure.That(fromVersionInclusive, nameof(fromVersionInclusive)).IsGte(-1);
            Ensure.That(maxCount, nameof(maxCount)).IsGte(1);

            GuardAgainstDisposed();

            var client = CreateClient();

            var reader = new StreamBackwardsReader(
                client,
                streamId,
                fromVersionInclusive,
                maxCount,
                prefetchJsonData);

            IAsyncEnumerable<StreamMessage> NoOp(StreamMessage[] messages, CancellationToken ct)
                => messages.ToAsyncEnumerable();

            return new ReadStreamResult(reader.Read, NoOp, cancellationToken);
        }

        private abstract class StreamReader
        {
            private IHalClient _client;
            private readonly StreamId _streamId;
            private readonly int _fromVersionInclusive;
            private readonly int _pageSize;
            private readonly bool _prefetchJsonData;
            private readonly Func<StreamId, int, int, bool, string> _formatLink;
            private IResource _resource;
            private readonly ReadDirection _readDirection;

            protected StreamReader(
                IHalClient client,
                StreamId streamId,
                int fromVersionInclusive,
                int pageSize,
                bool prefetchJsonData,
                ReadDirection readDirection,
                Func<StreamId, int, int, bool, string> formatLink)
            {
                _client = client;
                _streamId = streamId;
                _fromVersionInclusive = fromVersionInclusive;
                _pageSize = pageSize;
                _prefetchJsonData = prefetchJsonData;
                _formatLink = formatLink;
                _readDirection = readDirection;
            }

            public async Task<ReadStreamPage> Read(CancellationToken cancellationToken)
            {
                _client = await _client.RootAsync(
                    _formatLink(_streamId, _fromVersionInclusive, _pageSize, _prefetchJsonData),
                    cancellationToken);

                return ReadInternal();
            }

            private async Task<ReadStreamPage> ReadNext(int _, CancellationToken cancellationToken)
            {
                _client = await _client.GetAsync(_resource, Constants.Relations.Previous, cancellationToken);

                return ReadInternal();
            }

            private ReadStreamPage ReadInternal()
            {
                _resource = _client.Current.First();

                var pageInfo = _resource.Data<HalReadPage>();

                var streamMessages = Convert(
                    _resource.Embedded
                        .Where(r => r.Rel == Constants.Relations.Message)
                        .ToArray(),
                    _client,
                    _prefetchJsonData);

                return new ReadStreamPage(
                    _streamId,
                    _client.StatusCode < HttpStatusCode.NotFound
                        ? PageReadStatus.Success
                        : PageReadStatus.StreamNotFound,
                    pageInfo.FromStreamVersion,
                    pageInfo.NextStreamVersion,
                    pageInfo.LastStreamVersion,
                    pageInfo.LastStreamPosition,
                    _readDirection,
                    pageInfo.IsEnd,
                    ReadNext,
                    streamMessages);
            }
        }

        private class StreamBackwardsReader : StreamReader
        {
            public StreamBackwardsReader(
                IHalClient client,
                StreamId streamId,
                int fromVersionInclusive,
                int pageSize,
                bool prefetchJsonData)
                : base(
                    client,
                    streamId,
                    fromVersionInclusive,
                    pageSize,
                    prefetchJsonData,
                    ReadDirection.Backward,
                    LinkFormatter.ReadStreamBackwards)
            { }
        }

        private class StreamForwardsReader : StreamReader
        {
            public StreamForwardsReader(
                IHalClient client,
                StreamId streamId,
                int fromVersionInclusive,
                int pageSize,
                bool prefetchJsonData)
                : base(
                    client,
                    streamId,
                    fromVersionInclusive,
                    pageSize,
                    prefetchJsonData,
                    ReadDirection.Forward,
                    LinkFormatter.ReadStreamForwards)
            { }
        }
    }
}