namespace Example
{
    using System;
    using System.Threading;
    using System.Threading.Tasks;
    using EasyConsole;
    using SqlStreamStore;

    public abstract class Example
    {
        public void Run(CancellationToken cancellationToken)
        {
            Task.Run(() => RunAsync(cancellationToken)).GetAwaiter().GetResult();
        }

        protected abstract Task RunAsync(CancellationToken cancellationToken);

        protected IStreamStore GetStore()
        {
            IStreamStore streamStore = null;

            Output.WriteLine(ConsoleColor.Yellow, "Store type:");
            new Menu()
                .Add("InMem", () => streamStore = new InMemoryStreamStore())
                .Add("MS SQL (SqlLocalDB, NamedPipes)", () => streamStore = new MsSqlStreamStoreFixture("dbo").GetStreamStore().Result)
                .Display();

            return streamStore;
        }
    }
}