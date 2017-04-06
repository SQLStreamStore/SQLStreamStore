using System;

using Xunit.Abstractions;

namespace SqlStreamStore {
	public partial class StreamStoreAcceptanceTests {
		private StreamStoreAcceptanceTestFixture GetFixture(string schema = "foo") {
			return new SQLiteStreamStoreFixture(schema);
		}

		private IDisposable CaptureLogs(ITestOutputHelper testOutputHelper) {
			throw new NotImplementedException();
			//return LoggingHelper.Capture(testOutputHelper);
		}
	}
}
