namespace SqlStreamStore.Streams
{
    using System;
    using System.Collections.Generic;
    using System.Threading;
    using System.Threading.Tasks;

    public class ReadStreamResult : IAsyncEnumerable<StreamMessage>
    {
        private readonly Func<CancellationToken, Task<ReadStreamPage>> _readPage;
        private readonly Func<StreamMessage[], CancellationToken, IAsyncEnumerable<StreamMessage>> _filterExpired;
        private readonly CancellationToken _disposed;

        /// <summary>
        ///     The version of the stream that read from.
        /// </summary>
        public int FromStreamVersion { get; private set; }

        /// <summary>
        ///     Whether or not this is the end of the stream.
        /// </summary>
        public bool IsEnd { get; private set; }

        /// <summary>
        ///     The version of the last message in the stream.
        /// </summary>
        public int LastStreamVersion { get; private set; }

        /// <summary>
        ///     The position of the last message in the stream.
        /// </summary>
        public long LastStreamPosition { get; private set; }

        /// <summary>
        ///     The next message version that can be read.
        /// </summary>
        public int NextStreamVersion { get; private set; }

        /// <summary>
        ///     The direction of the read operation.
        /// </summary>
        public ReadDirection ReadDirection { get; private set; }

        /// <summary>
        ///     The <see cref="PageReadStatus"/> of the read operation.
        /// </summary>
        public PageReadStatus Status { get; private set; }

        /// <summary>
        ///     The id of the stream that was read.
        /// </summary>
        public string StreamId { get; private set; }

        /// <summary>
        /// Initializes a new instance of <see cref="ReadAllResult"/>
        /// </summary>
        /// <param name="readPage">A function to return the initial page of results.</param>
        /// <param name="filterExpired">A function to filter out messages due to expiration.</param>
        /// <param name="disposed">The cancellation token.</param>
        /// <exception cref="ArgumentNullException"></exception>
        public ReadStreamResult(
            Func<CancellationToken, Task<ReadStreamPage>> readPage,
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
                ReadDirection = page.ReadDirection;
                IsEnd = page.IsEnd;
                Status = page.Status;
                StreamId = page.StreamId;
                FromStreamVersion = page.FromStreamVersion;
                LastStreamPosition = page.LastStreamPosition;
                LastStreamVersion = page.LastStreamVersion;
                NextStreamVersion = page.NextStreamVersion;

                while(page.Messages.Length > 0)
                {
                    await foreach(var message in _filterExpired(page.Messages, ct).WithCancellation(ct))
                    {
                        if(message.StreamVersion > LastStreamVersion)
                        {
                            break;
                        }

                        yield return message;

                        GuardAgainstDisposed();
                    }

                    if(page.IsEnd)
                    {
                        break;
                    }

                    page = await page.ReadNext(ct);
                    NextStreamVersion = page.NextStreamVersion;
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