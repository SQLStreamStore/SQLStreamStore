namespace Cedar.EventStore
{
    using System.Threading.Tasks;
    using SQLite.Net.Platform.Win32;

    public class SqliteEventStoreFixture : EventStoreAcceptanceTestFixture
    {
        readonly string databaseFile = @"c:\temp\test.sdb";

        public override Task<IEventStore> GetEventStore()
        {
            /*if(File.Exists(databaseFile))
            {
                File.Delete(databaseFile);
            }*/
            var eventStore = new SqliteEventStore(new SQLitePlatformWin32(), databaseFile);
            
            eventStore.Drop();
            eventStore.Initialize();

            return Task.FromResult((IEventStore)eventStore);
        }

        public override void Dispose()
        {}
    }
}