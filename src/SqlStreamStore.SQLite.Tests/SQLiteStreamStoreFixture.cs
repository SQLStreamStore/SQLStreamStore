using System;
using System.Threading.Tasks;
using SqlStreamStore.SQLite;

namespace SqlStreamStore {
	public class SQLiteStreamStoreFixture : StreamStoreAcceptanceTestFixture {
		readonly string _schema;

		public SQLiteStreamStoreFixture(string schema) {
			_schema = schema;
		}

		public override Task<IStreamStore> GetStreamStore() {
			throw new NotImplementedException();
		}

		public async Task<IStreamStore> GetStreamStore(string schema) {
			throw new NotImplementedException();
		}

		public async Task<SQLiteStreamStore> GetSQLiteStreamStore() {
			throw new NotImplementedException();
		}

		public override void Dispose() {
			base.Dispose();
		}
	}
}
