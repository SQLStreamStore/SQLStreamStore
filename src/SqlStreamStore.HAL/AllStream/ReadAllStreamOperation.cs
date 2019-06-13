namespace SqlStreamStore.HAL.AllStream
{
    using System.Threading;
    using System.Threading.Tasks;
    using Microsoft.AspNetCore.Http;
    using SqlStreamStore.Streams;

    internal class ReadAllStreamOperation : IStreamStoreOperation<ReadAllPage>
    {
        private readonly long _fromPositionInclusive;
        private readonly int _maxCount;

        public ReadAllStreamOperation(HttpContext context)
        {
            var request = context.Request;
            
            Path = request.Path;

            EmbedPayload = request.Query.TryGetValueCaseInsensitive('e', out var embedPayload)
                           && embedPayload == "1";

            ReadDirection = request.Query.TryGetValueCaseInsensitive('d', out var readDirection)
                            && readDirection == "f" || readDirection == "F"
                ? Constants.ReadDirection.Forwards
                : Constants.ReadDirection.Backwards;

            _fromPositionInclusive = request.Query.TryGetValueCaseInsensitive('p', out var position)
                ? (long.TryParse(position, out _fromPositionInclusive)
                    ? (_fromPositionInclusive < Position.End
                        ? Position.End
                        : _fromPositionInclusive)
                    : (ReadDirection == Constants.ReadDirection.Forwards
                        ? Position.Start
                        : Position.End))
                : (ReadDirection == Constants.ReadDirection.Forwards
                    ? Position.Start
                    : Position.End);

            _maxCount = request.Query.TryGetValueCaseInsensitive('m', out var maxCount)
                ? (int.TryParse(maxCount, out _maxCount)
                    ? (_maxCount <= 0
                        ? Constants.MaxCount
                        : _maxCount)
                    : Constants.MaxCount)
                : Constants.MaxCount;

            Self = ReadDirection == Constants.ReadDirection.Forwards
                ? LinkFormatter.ReadAllForwards(
                    FromPositionInclusive,
                    MaxCount,
                    EmbedPayload)
                : LinkFormatter.ReadAllBackwards(
                    FromPositionInclusive,
                    MaxCount,
                    EmbedPayload);

            IsUriCanonical = Self.Remove(0, LinkFormatter.AllStream().Length)
                             == request.QueryString.ToUriComponent();
        }

        public long FromPositionInclusive => _fromPositionInclusive;
        public int MaxCount => _maxCount;
        public bool EmbedPayload { get; }
        public int ReadDirection { get; }
        public string Self { get; }
        public bool IsUriCanonical { get; }
        public PathString Path { get; }

        public Task<ReadAllPage> Invoke(IStreamStore streamStore, CancellationToken ct)
            => ReadDirection == Constants.ReadDirection.Forwards
                ? streamStore.ReadAllForwards(_fromPositionInclusive, _maxCount, EmbedPayload, ct)
                : streamStore.ReadAllBackwards(_fromPositionInclusive, _maxCount, EmbedPayload, ct);
    }
}