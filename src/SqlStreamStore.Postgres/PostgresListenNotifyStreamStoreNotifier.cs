namespace SqlStreamStore
{
    using System;
    using System.Threading;
    using System.Threading.Tasks;
    using Npgsql;
    using SqlStreamStore.Infrastructure;
    using SqlStreamStore.Subscriptions;

    public sealed class PostgresListenNotifyStreamStoreNotifier : IStreamStoreNotifier
    {
        private readonly PostgresStreamStoreSettings _settings;
        private readonly Subject<Unit> _notificationReceived;
        private readonly CancellationTokenSource _disposed;

        public PostgresListenNotifyStreamStoreNotifier(PostgresStreamStoreSettings settings)
        {
            if(settings == null)
            {
                throw new ArgumentNullException(nameof(settings));
            }
            _settings = settings;
            _notificationReceived = new Subject<Unit>();
            _disposed = new CancellationTokenSource();
            Task.Run(ReceiveNotify, _disposed.Token);
        }

        private async Task ReceiveNotify()
        {
            using(var connection = _settings.ConnectionFactory(_settings.ConnectionString))
            {
                await connection.OpenAsync(_disposed.Token);
                connection.Notification += ConnectionOnNotification;

                using(var command = new NpgsqlCommand($"LISTEN on_append_{_settings.Schema}", connection))
                {
                    await command.ExecuteNonQueryAsync(_disposed.Token);
                }
                while(!_disposed.IsCancellationRequested)
                {
                    await connection.WaitAsync(_disposed.Token);
                }
            }
        }

        private void ConnectionOnNotification(object sender, NpgsqlNotificationEventArgs e)
            => _notificationReceived.OnNext(Unit.Default);

        public IDisposable Subscribe(IObserver<Unit> observer) => _notificationReceived.Subscribe(observer);

        public void Dispose() => _disposed.Dispose();
    }
}