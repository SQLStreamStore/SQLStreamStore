using System;

using EnsureThat;

using SqlStreamStore.Infrastructure;
using SqlStreamStore.Subscriptions;

namespace SqlStreamStore.SQLite {
	public class SQLiteStreamStoreSettings {
		public string ConnectionString { get; private set; }

		public CreateStreamStoreNotifier CreateStreamStoreNotifier { get; set; } = store => new PollingStreamStoreNotifier(store);

		public TimeSpan MetadataMaxAgeCacheExpire { get; set; } = TimeSpan.FromMinutes(1);

		public int MetadataMaxAgeCacheMaxSize { get; set; } = 10000;

		public GetUtcNow GetUtcNow { get; set; }

		public string LogName { get { return "SQLiteStreamStore"; } }

		public SQLiteStreamStoreSettings(string connectionString) {
			Ensure.That(connectionString, nameof(connectionString)).IsNotNullOrWhiteSpace();

			ConnectionString = connectionString;
		}
	}
}
