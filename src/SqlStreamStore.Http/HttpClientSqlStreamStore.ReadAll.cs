namespace SqlStreamStore
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Threading;
    using System.Threading.Tasks;
    using SqlStreamStore.Imports.Ensure.That;
    using SqlStreamStore.Internal.HoneyBearHalClient;
    using SqlStreamStore.Internal.HoneyBearHalClient.Models;
    using SqlStreamStore.Streams;


    partial class HttpClientSqlStreamStore
    {
        public ReadAllResult ReadAllForwards(
            long fromPositionInclusive,
            int maxCount,
            bool prefetchJsonData = true,
            CancellationToken cancellationToken = default)
        {
            Ensure.That(fromPositionInclusive, nameof(fromPositionInclusive)).IsGte(0);
            Ensure.That(maxCount, nameof(maxCount)).IsGte(1);

            GuardAgainstDisposed();

            var client = CreateClient();

            var reader = new AllStreamForwardsReader(
                client,
                fromPositionInclusive,
                maxCount,
                prefetchJsonData);

            IAsyncEnumerable<StreamMessage> NoOp(StreamMessage[] messages, CancellationToken ct)
                => messages.ToAsyncEnumerable();

            return new ReadAllResult(reader.Read, NoOp, cancellationToken);
        }

        public ReadAllResult ReadAllBackwards(
            long fromPositionInclusive,
            int maxCount,
            bool prefetchJsonData = true,
            CancellationToken cancellationToken = default)
        {
            Ensure.That(fromPositionInclusive, nameof(fromPositionInclusive)).IsGte(-1);
            Ensure.That(maxCount, nameof(maxCount)).IsGte(1);

            GuardAgainstDisposed();

            var client = CreateClient();

            var reader = new AllStreamBackwardsReader(
                client,
                fromPositionInclusive,
                maxCount,
                prefetchJsonData);

            IAsyncEnumerable<StreamMessage> NoOp(StreamMessage[] messages, CancellationToken ct)
                => messages.ToAsyncEnumerable();

            return new ReadAllResult(reader.Read, NoOp, cancellationToken);
        }

        private abstract class AllStreamReader
        {
            private IHalClient _client;
            private readonly long _fromPositionInclusive;
            private readonly int _pageSize;
            private readonly bool _prefetchJsonData;
            private readonly Func<long, int, bool, string> _formatLink;
            private IResource _resource;
            private readonly ReadDirection _readDirection;

            protected AllStreamReader(
                IHalClient client,
                long fromPositionInclusive,
                int pageSize,
                bool prefetchJsonData,
                ReadDirection readDirection,
                Func<long, int, bool, string> formatLink)
            {
                _client = client;
                _fromPositionInclusive = fromPositionInclusive;
                _pageSize = pageSize;
                _prefetchJsonData = prefetchJsonData;
                _formatLink = formatLink;
                _readDirection = readDirection;
            }

            public async Task<ReadAllPage> Read(CancellationToken cancellationToken)
            {
                _client = await _client.RootAsync(
                    _formatLink(_fromPositionInclusive, _pageSize, _prefetchJsonData),
                    cancellationToken);

                return ReadInternal();
            }

            private async Task<ReadAllPage> ReadNext(long _, CancellationToken cancellationToken)
            {
                _client = await _client.GetAsync(_resource, Constants.Relations.Previous, cancellationToken);

                return ReadInternal();
            }

            private ReadAllPage ReadInternal()
            {
                _resource = _client.Current.First();

                var pageInfo = _resource.Data<HalReadAllPage>();

                var streamMessages = Convert(
                    _resource.Embedded
                        .Where(r => r.Rel == Constants.Relations.Message)
                        .ToArray(),
                    _client,
                    _prefetchJsonData);

                return new ReadAllPage(
                    pageInfo.FromPosition,
                    pageInfo.NextPosition,
                    pageInfo.IsEnd,
                    _readDirection,
                    ReadNext,
                    streamMessages);
            }
        }

        private class AllStreamBackwardsReader : AllStreamReader
        {
            public AllStreamBackwardsReader(
                IHalClient client,
                long fromPositionInclusive,
                int pageSize,
                bool prefetchJsonData)
                : base(
                    client,
                    fromPositionInclusive,
                    pageSize,
                    prefetchJsonData,
                    ReadDirection.Backward,
                    LinkFormatter.ReadAllBackwards)
            { }
        }

        private class AllStreamForwardsReader : AllStreamReader
        {
            public AllStreamForwardsReader(
                IHalClient client,
                long fromPositionInclusive,
                int pageSize,
                bool prefetchJsonData)
                : base(
                    client,
                    fromPositionInclusive,
                    pageSize,
                    prefetchJsonData,
                    ReadDirection.Forward,
                    LinkFormatter.ReadAllForwards)
            { }
        }
    }
}