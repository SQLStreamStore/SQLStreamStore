namespace SqlStreamStore.Streams
{
    using System;
    using System.Collections.Generic;
    using System.Threading;
    using System.Threading.Tasks;

    public class ReadAllResult : IAsyncEnumerable<StreamMessage>
    {
        private readonly Func<CancellationToken, Task<ReadAllPage>> _readPage;
        private readonly Func<StreamMessage[], CancellationToken, IAsyncEnumerable<StreamMessage>> _filterExpired;
        private readonly CancellationToken _disposed;

        /// <summary>
        ///     Whether or not this is the end of the stream.
        /// </summary>
        public bool IsEnd { get; private set; }

        /// <summary>
        ///     A long representing the position where this page was read from.
        /// </summary>
        public long FromPosition { get; private set; }

        /// <summary>
        ///     A long representing the position where the next page should be read from.
        /// </summary>
        public long NextPosition { get; private set; }

        /// <summary>
        ///     The direction of the read operation.
        /// </summary>
        public ReadDirection ReadDirection { get; private set; }

        /// <summary>
        /// Initializes a new instance of <see cref="ReadAllResult"/>
        /// </summary>
        /// <param name="readPage">A function to return the initial page of results.</param>
        /// <param name="filterExpired">A function to filter out messages due to expiration.</param>
        /// <param name="disposed">The cancellation token.</param>
        /// <exception cref="ArgumentNullException"></exception>
        public ReadAllResult(
            Func<CancellationToken, Task<ReadAllPage>> readPage,
            Func<StreamMessage[], CancellationToken, IAsyncEnumerable<StreamMessage>> filterExpired,
            CancellationToken disposed)
        {
            if(readPage == null)
            {
                throw new ArgumentNullException(nameof(readPage));
            }

            if(filterExpired == null)
            {
                throw new ArgumentNullException(nameof(filterExpired));
            }

            _readPage = readPage;
            _filterExpired = filterExpired;
            _disposed = disposed;
        }

        public IAsyncEnumerator<StreamMessage> GetAsyncEnumerator(CancellationToken cancellationToken = default)
        {
            async IAsyncEnumerator<StreamMessage> Enumerator(CancellationToken ct)
            {
                GuardAgainstDisposed();

                var page = await _readPage(ct);
                ReadDirection = page.Direction;
                FromPosition = page.FromPosition;
                NextPosition = page.NextPosition;
                IsEnd = page.IsEnd;

                while(page.Messages.Length > 0)
                {
                    await foreach(var message in _filterExpired(page.Messages, ct).WithCancellation(ct))
                    {
                        yield return message;

                        GuardAgainstDisposed();
                    }

                    if(page.IsEnd)
                    {
                        break;
                    }

                    page = await page.ReadNext(ct);
                    FromPosition = page.FromPosition;
                    NextPosition = page.NextPosition;
                    IsEnd = page.IsEnd;
                }
            }

            return Enumerator(cancellationToken);
        }

        private void GuardAgainstDisposed()
        {
            if(_disposed.IsCancellationRequested)
            {
                throw new ObjectDisposedException(nameof(ReadAllResult));
            }
        }
    }
}