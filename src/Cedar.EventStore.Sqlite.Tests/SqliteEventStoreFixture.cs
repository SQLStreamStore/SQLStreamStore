namespace Cedar.EventStore
{
    using System.Threading.Tasks;
    using SQLite.Net.Platform.Win32;

    public class SqliteEventStoreFixture : EventStoreAcceptanceTestFixture
    {
        readonly string databaseFile = @"c:\temp\test.sdb";

        public override Task<IEventStoreClient> GetEventStore()
        {
            /*if(File.Exists(databaseFile))
            {
                File.Delete(databaseFile);
            }*/
            var eventStore = new SqliteEventStoreClient(new SQLitePlatformWin32(), databaseFile);
            
            eventStore.Drop();
            eventStore.Initialize();

            return Task.FromResult((IEventStoreClient)eventStore);
        }

        public override void Dispose()
        {}
    }
}