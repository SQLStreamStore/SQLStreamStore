namespace SqlStreamStore.Streams
{
    using System.Threading;
    using System.Threading.Tasks;
    using Microsoft.AspNetCore.Http;
    using Microsoft.AspNetCore.Routing;

    internal class ReadStreamOperation : IStreamStoreOperation<ReadStreamResult>
    {
        private readonly int _fromVersionInclusive;
        private readonly int _maxCount;

        public ReadStreamOperation(HttpContext context)
        {
            var request = context.Request;
            Path = request.Path;

            StreamId = context.GetRouteData().GetStreamId();
            EmbedPayload = request.Query.TryGetValueCaseInsensitive('e', out var embedPayload)
                           && embedPayload == "1";
            ReadDirection = request.Query.TryGetValueCaseInsensitive('d', out var readDirection)
                            && readDirection == "f" || readDirection == "F"
                ? Constants.ReadDirection.Forwards
                : Constants.ReadDirection.Backwards;

            _fromVersionInclusive = request.Query.TryGetValueCaseInsensitive('p', out var position)
                ? int.TryParse(position, out _fromVersionInclusive)
                    ? ReadDirection == Constants.ReadDirection.Forwards
                        ? _fromVersionInclusive < StreamVersion.Start
                            ? StreamVersion.Start
                            : _fromVersionInclusive
                        : _fromVersionInclusive < StreamVersion.End
                            ? StreamVersion.End
                            : _fromVersionInclusive
                    : ReadDirection == Constants.ReadDirection.Forwards
                        ? StreamVersion.Start
                        : StreamVersion.End
                : ReadDirection == Constants.ReadDirection.Forwards
                    ? StreamVersion.Start
                    : StreamVersion.End;

            _maxCount = request.Query.TryGetValueCaseInsensitive('m', out var maxCount)
                ? int.TryParse(maxCount, out _maxCount)
                    ? _maxCount <= 0
                        ? Constants.MaxCount
                        : _maxCount
                    : Constants.MaxCount
                : Constants.MaxCount;

            var baseAddress = LinkFormatter.Stream(StreamId);

            Self = ReadDirection == Constants.ReadDirection.Forwards
                ? LinkFormatter.ReadStreamForwards(StreamId, FromVersionInclusive, MaxCount, EmbedPayload)
                : LinkFormatter.ReadStreamBackwards(StreamId, FromVersionInclusive, MaxCount, EmbedPayload);

            IsUriCanonical = Self.Remove(0, baseAddress.Length)
                             == request.QueryString.ToUriComponent();
        }

        public PathString Path { get; }
        public int FromVersionInclusive => _fromVersionInclusive;
        public int MaxCount => _maxCount;
        public bool EmbedPayload { get; }
        public int ReadDirection { get; }
        public string StreamId { get; }
        public string Self { get; }
        public bool IsUriCanonical { get; }

        public Task<ReadStreamResult> Invoke(IStreamStore streamStore, CancellationToken ct)
            => Task.FromResult(ReadDirection == Constants.ReadDirection.Forwards
                ? streamStore.ReadStreamForwards(StreamId, _fromVersionInclusive, _maxCount, EmbedPayload, ct)
                : streamStore.ReadStreamBackwards(StreamId, _fromVersionInclusive, _maxCount, EmbedPayload, ct));
    }
}