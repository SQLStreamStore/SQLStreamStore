namespace SqlStreamStore
{
    using System;
    using System.Data;
    using System.Threading;
    using System.Threading.Tasks;
    using Npgsql;
    using SqlStreamStore.Infrastructure;
    using SqlStreamStore.Logging;
    using SqlStreamStore.PgSqlScripts;
    using SqlStreamStore.Subscriptions;

    public sealed class PostgresListenNotifyStreamStoreNotifier : IStreamStoreNotifier
    {
        private static readonly ILog s_Log = LogProvider.For<PostgresListenNotifyStreamStoreNotifier>();

        private readonly PostgresStreamStoreSettings _settings;
        private readonly Subject<Unit> _notificationReceived;
        private readonly CancellationTokenSource _disposed;
        private readonly Schema _schema;
        private string ConnectionString => new NpgsqlConnectionStringBuilder(_settings.ConnectionString)
        {
            KeepAlive = 5,
            Pooling = false
        }.ConnectionString;


        public PostgresListenNotifyStreamStoreNotifier(PostgresStreamStoreSettings settings)
        {
            if(settings == null)
            {
                throw new ArgumentNullException(nameof(settings));
            }

            _settings = settings;
            _schema = new Schema(_settings.Schema);
            _notificationReceived = new Subject<Unit>();
            _disposed = new CancellationTokenSource();
            Task.Run(ReceiveNotify, _disposed.Token);
        }

        private async Task ReceiveNotify()
        {
            while(!_disposed.IsCancellationRequested)
            {
                using(var connection = _settings.ConnectionFactory(ConnectionString))
                {
                    try
                    {
                        await connection.OpenAsync(_disposed.Token).NotOnCapturedContext();
                        connection.Notification += OnNotificationReceived;

                        using(var command = new NpgsqlCommand(_schema.Listen, connection)
                        {
                            CommandType = CommandType.StoredProcedure
                        })
                        {
                            s_Log.Info("Listening to notifications channel.");
                            await command.ExecuteNonQueryAsync(_disposed.Token).NotOnCapturedContext();
                        }

                        while(!_disposed.IsCancellationRequested)
                        {
                            await connection.WaitAsync(_disposed.Token).NotOnCapturedContext();
                        }
                    }
                    catch(ObjectDisposedException ex)
                    {
                        s_Log.WarnException("Receive notification failed. A new connection will not be opened.", ex);
                        throw;
                    }
                    catch(OperationCanceledException ex)
                    {
                        s_Log.WarnException("Receive notification failed. A new connection will not be opened.", ex);
                    }
                    catch(Exception ex)
                    {
                        s_Log.WarnException("Receive notification failed. A new connection will be opened.", ex);
                    }
                    finally
                    {
                        connection.Notification -= OnNotificationReceived;
                    }
                }
            }
        }

        private void OnNotificationReceived(object sender, NpgsqlNotificationEventArgs e)
        {
            s_Log.TraceFormat(
                "Notification received. {Condition} {AdditionalInformation}",
                e.Condition,
                e.AdditionalInformation);
            _notificationReceived.OnNext(Unit.Default);
        }

        public IDisposable Subscribe(IObserver<Unit> observer) => _notificationReceived.Subscribe(observer);

        public void Dispose() => _disposed.Cancel();
    }
}