using System;
using System.Threading;
using System.Threading.Tasks;
using EnsureThat;
using SqlStreamStore.Infrastructure;
using SqlStreamStore.Streams;
using SqlStreamStore.Subscriptions;

namespace SqlStreamStore.SQLite {
	public sealed partial class SQLiteStreamStore : StreamStoreBase {
		public SQLiteStreamStore(SQLiteStreamStoreSettings settings) : base(settings.MetadataMaxAgeCacheExpire, settings.MetadataMaxAgeCacheMaxSize, settings.GetUtcNow, settings.LogName) {
			Ensure.That(settings, nameof(settings)).IsNotNull();
		}

		public override Task<int> GetmessageCount(string streamId, CancellationToken cancellationToken = default(CancellationToken)) {
			throw new NotImplementedException();
		}

		protected override Task<AppendResult> AppendToStreamInternal(string streamId, int expectedVersion, NewStreamMessage[] messages, CancellationToken cancellationToken) {
			throw new NotImplementedException();
		}

		protected override Task DeleteEventInternal(string streamId, Guid eventId, CancellationToken cancellationToken) {
			throw new NotImplementedException();
		}

		protected override Task DeleteStreamInternal(string streamId, int expectedVersion, CancellationToken cancellationToken) {
			throw new NotImplementedException();
		}

		protected override Task<StreamMetadataResult> GetStreamMetadataInternal(string streamId, CancellationToken cancellationToken) {
			throw new NotImplementedException();
		}

		protected override Task<ReadAllPage> ReadAllBackwardsInternal(long fromPositionExclusive, int maxCount, bool prefetch, ReadNextAllPage readNext, CancellationToken cancellationToken) {
			throw new NotImplementedException();
		}

		protected override Task<ReadAllPage> ReadAllForwardsInternal(long fromPositionExlusive, int maxCount, bool prefetch, ReadNextAllPage readNext, CancellationToken cancellationToken) {
			throw new NotImplementedException();
		}

		protected override Task<long> ReadHeadPositionInternal(CancellationToken cancellationToken) {
			throw new NotImplementedException();
		}

		protected override Task<ReadStreamPage> ReadStreamBackwardsInternal(string streamId, int fromVersionInclusive, int count, bool prefetch, ReadNextStreamPage readNext, CancellationToken cancellationToken) {
			throw new NotImplementedException();
		}

		protected override Task<ReadStreamPage> ReadStreamForwardsInternal(string streamId, int start, int count, bool prefetch, ReadNextStreamPage readNext, CancellationToken cancellationToken) {
			throw new NotImplementedException();
		}

		protected override Task SetStreamMetadataInternal(string streamId, int expectedStreamMetadataVersion, int? maxAge, int? maxCount, string metadataJson, CancellationToken cancellationToken) {
			throw new NotImplementedException();
		}

		protected override IAllStreamSubscription SubscribeToAllInternal(long? fromPosition, AllStreamMessageReceived streamMessageReceived, AllSubscriptionDropped subscriptionDropped, HasCaughtUp hasCaughtUp, string name) {
			throw new NotImplementedException();
		}

		protected override IStreamSubscription SubscribeToStreamInternal(string streamId, int? startVersion, StreamMessageReceived streamMessageReceived, SubscriptionDropped subscriptionDropped, HasCaughtUp hasCaughtUp, string name) {
			throw new NotImplementedException();
		}
	}
}
