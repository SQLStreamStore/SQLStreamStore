namespace SqlStreamStore
{
    using System.Linq;
    using System.Threading;
    using System.Threading.Tasks;
    using Microsoft.Data.Sqlite;
    using SqlStreamStore.Streams;

    public partial class SqliteStreamStore
    {
        protected override async Task<ReadStreamPage> ReadStreamForwardsInternal(
            string streamId,
            int fromVersion,
            int count,
            bool prefetch,
            ReadNextStreamPage readNext,
            CancellationToken cancellationToken)
        {
            GuardAgainstDisposed();
            cancellationToken.ThrowIfCancellationRequested();

            // If the count is int.MaxValue, TSql will see it as a negative number. 
            // Users shouldn't be using int.MaxValue in the first place anyway.
            var maxRecords = count == int.MaxValue ? count - 1 : count;

            using(var connection = OpenConnection())
            {
                var streamProperties = await connection.Streams(streamId)
                    .Properties(initializeIfNotFound:false, cancellationToken);

                if(streamProperties == null)
                {
                    // not found.
                    return new ReadStreamPage(
                        streamId,
                        PageReadStatus.StreamNotFound,
                        fromVersion,
                        StreamVersion.End,
                        StreamVersion.End,
                        StreamVersion.End,
                        ReadDirection.Forward,
                        true,
                        readNext);
                }
                
                return await PrepareStreamResponse(connection,
                    streamId,
                    ReadDirection.Forward,
                    fromVersion,
                    prefetch,
                    readNext,
                    maxRecords,
                    streamProperties.MaxAge);
            }
        }

        protected override async Task<ReadStreamPage> ReadStreamBackwardsInternal(
            string streamId,
            int fromStreamVersion,
            int count,
            bool prefetch,
            ReadNextStreamPage readNext,
            CancellationToken cancellationToken)
        {
            GuardAgainstDisposed();
            cancellationToken.ThrowIfCancellationRequested();

            // If the count is int.MaxValue, TSql will see it as a negative number. 
            // Users shouldn't be using int.MaxValue in the first place anyway.
            var maxRecords = count == int.MaxValue ? count - 1 : count;
            var streamVersion = fromStreamVersion == StreamVersion.End ? int.MaxValue - 1 : fromStreamVersion;
            using(var connection = OpenConnection())
            {
                var streamProperties = await connection.Streams(streamId)
                    .Properties(initializeIfNotFound: false, cancellationToken);

                if(streamProperties == null)
                {
                    // not found.
                    return new ReadStreamPage(
                        streamId,
                        PageReadStatus.StreamNotFound,
                        fromStreamVersion,
                        StreamVersion.End,
                        StreamVersion.End,
                        StreamVersion.End,
                        ReadDirection.Forward,
                        true,
                        readNext);
                }

                var position = connection.Streams(streamId)
                    .AllStreamPosition(ReadDirection.Backward, streamVersion);

                // if no position, then need to return success with end of stream.
                if(position == null)
                {
                    // not found.
                    return new ReadStreamPage(
                        streamId,
                        PageReadStatus.Success,
                        fromStreamVersion,
                        StreamVersion.End,
                        StreamVersion.End,
                        StreamVersion.End,
                        ReadDirection.Backward,
                        true,
                        readNext);
                }

                return await PrepareStreamResponse(connection,
                    streamId,
                    ReadDirection.Backward,
                    fromStreamVersion,
                    prefetch,
                    readNext,
                    maxRecords,
                    streamProperties.MaxAge);
            }
        }

        private Task<ReadStreamPage> PrepareStreamResponse(
            SqliteConnection connection,
            string streamId,
            ReadDirection direction,
            int fromVersion,
            bool prefetch,
            ReadNextStreamPage readNext,
            int maxRecords,
            int? maxAge)
        {
            var streamVersion = fromVersion == StreamVersion.End ? int.MaxValue -1 : fromVersion;
            int nextVersion = 0;
            var stream = connection.Streams(streamId);

            var header = stream
                .Properties()
                .GetAwaiter().GetResult();

            var position = stream
                .AllStreamPosition(direction, streamVersion)
                .GetAwaiter().GetResult();

            var remaining = stream
                .Length(direction, position, CancellationToken.None)
                .GetAwaiter().GetResult();

            var messages = stream
                .Read(direction, position, prefetch, maxRecords)
                .GetAwaiter().GetResult()
                .Select(Message => (Message, maxAge))
                .ToList();

            var filtered = FilterExpired(messages);

            var isEnd = remaining - messages.Count <= 0;

            if(direction == ReadDirection.Forward)
            {
                if(messages.Any())
                {
                    nextVersion = messages.Last().Message.StreamVersion + 1;
                }
                else
                {
                    nextVersion = header.Version + 1;
                }
            }
            else if (direction == ReadDirection.Backward)
            {
                if(streamVersion == int.MaxValue - 1 && !messages.Any())
                {
                    nextVersion = StreamVersion.End;
                }

                if(messages.Any())
                {
                    nextVersion = messages.Last().Message.StreamVersion - 1;
                }
            }

            var page = new ReadStreamPage(
                streamId,
                status: PageReadStatus.Success,
                fromStreamVersion: fromVersion,
                nextStreamVersion: nextVersion,
                lastStreamVersion: header.Version,
                lastStreamPosition: header.Position,
                direction: direction,
                isEnd: isEnd,
                readNext: readNext,
                messages: filtered.ToArray());

            return Task.FromResult(page);
        }
    }
}