namespace SqlStreamStore.Oracle
{
    using System;
    using System.Collections.Generic;
    using System.Data;
    using System.Linq;
    using System.Threading;
    using System.Threading.Tasks;
    using global::Oracle.ManagedDataAccess.Client;
    using global::Oracle.ManagedDataAccess.Types;
    using SqlStreamStore.Infrastructure;
    using SqlStreamStore.Oracle.Database;
    using SqlStreamStore.OracleDatabase;
    using SqlStreamStore.Streams;
    using SqlStreamStore.Subscriptions;

    public sealed partial class OracleStreamStore : StreamStoreBase
    {
        private readonly OracleDbObjects _dbObjects;
        private readonly CommandBuilder _commandBuilder;

        private readonly OracleStreamStoreSettings _settings;
        
        private readonly Func<OracleConnection> _createConnection;
        private readonly Lazy<IStreamStoreNotifier> _streamStoreNotifier;

        public OracleStreamStore(OracleStreamStoreSettings settings) : base(settings.GetUtcNow, settings.LogName)
        {
            _settings = settings;
            _dbObjects = new OracleDbObjects(settings.Schema);
            _commandBuilder = new CommandBuilder(_dbObjects, settings.GetUtcNow); 
            _createConnection = () => settings.ConnectionFactory(settings.ConnectionString);
            _streamStoreNotifier = new Lazy<IStreamStoreNotifier>(() =>
            {
                if(settings.CreateStreamStoreNotifier == null)
                {
                    throw new InvalidOperationException(
                        "Cannot create notifier because supplied createStreamStoreNotifier was null");
                }
                return settings.CreateStreamStoreNotifier.Invoke(this);
            });
        }

        protected override async Task<long> ReadHeadPositionInternal(CancellationToken cancellationToken)
        {
            GuardAgainstDisposed();
            
            using(var conn = await OpenConnection(cancellationToken))
            using(var command = new OracleCommand($"SELECT MAX({_dbObjects.TableStreamEvents}.Position) FROM {_dbObjects.TableStreamEvents}", conn))
            {
                var result = await command
                    .ExecuteScalarAsync(cancellationToken)
                    .NotOnCapturedContext();
                
                if(result == DBNull.Value)
                {
                    return -1;
                }
                return Convert.ToInt64((Decimal)result);
            }
        }

        
        protected override async Task<ListStreamsPage> ListStreamsInternal(
            Pattern pattern,
            int maxCount,
            string continuationToken,
            ListNextStreamsPage listNextStreamsPage,
            CancellationToken cancellationToken)
        {
            if(!int.TryParse(continuationToken, out var afterIdInternal))
            {
                afterIdInternal = -1;
            }

            var patternSql = "";
            switch(pattern)
            {
                case Pattern.Any _:
                    patternSql = "%";
                    break;

                case Pattern.StartingWith p:
                    patternSql = $"{p.Value.Replace("_", "\\_").Replace("%", "\\%")}%";
                    break;
                
                case Pattern.EndingWith p:
                    patternSql = $"%{p.Value.Replace("_", "\\_").Replace("%", "\\%")}";
                    break;

                default:
                    throw Pattern.Unrecognized(nameof(pattern));
            }
            
            var streamIds = new List<string>();
            
            using(var connection = await OpenConnection(cancellationToken))
            using (var command = new OracleCommand())
            {
                command.BindByName = true;
                command.Connection = connection;
                command.CommandText =
                    $"SELECT {_dbObjects.TableStreams}.IdOriginal, {_dbObjects.TableStreams}.IdInternal " +
                    $"FROM {_dbObjects.TableStreams} " +
                    $"WHERE {_dbObjects.TableStreams}.IdInternal > :afterId ";
                    
                command.Parameters.Add(new OracleParameter(":afterid", OracleDbType.Int32, 10, afterIdInternal, ParameterDirection.Input));
                
                if(patternSql != "%")
                {
                    command.CommandText += $"AND {_dbObjects.TableStreams}.IdOriginal LIKE :pattern ESCAPE '\\' ";
                    command.Parameters.Add(new OracleParameter(":pattern", OracleDbType.NVarchar2, patternSql, ParameterDirection.Input));
                }
                
                command.CommandText += $"ORDER BY {_dbObjects.TableStreams}.IdInternal ASC " + 
                                       $"OFFSET 0 ROWS FETCH FIRST :maxCount ROWS ONLY";
                command.Parameters.Add(new OracleParameter(":maxCount", OracleDbType.Int32, 10, maxCount, ParameterDirection.Input));

                using(var reader = await command.ExecuteReaderAsync(cancellationToken).NotOnCapturedContext())
                {
                    while(await reader.ReadAsync(cancellationToken).NotOnCapturedContext())
                    {
                        streamIds.Add(reader.GetString(0));
                        afterIdInternal = Convert.ToInt32(reader.GetInt64(1));
                    }
                }

                return new ListStreamsPage(afterIdInternal.ToString(), streamIds.ToArray(), listNextStreamsPage);
            }
        }

        private async Task HandleDeletedEventsFeedback(
            OracleTransaction transaction,
            OracleCommand command,
            OracleRefCursor cursor,
            CancellationToken cancellationToken)
        {
            if(cursor.IsNull)
                return;
            
            OracleDataAdapter adapter = new OracleDataAdapter(command);
                
            DataSet dsResult = new DataSet();
            adapter.Fill(dsResult, "oDeletedEvents",  cursor);

            var deletedMessages = dsResult
                .Tables["oDeletedEvents"]
                .AsEnumerable()
                .Select(row => (row.Field<string>("StreamIdOriginal"), Guid.Parse(row.Field<string>("Id"))))
                .ToArray();

            await AppendDeletedMessages(transaction, deletedMessages, cancellationToken);
        }
        
        private async Task<OracleConnection> OpenConnection(CancellationToken cancellationToken)
        {
            var connection = _createConnection();
            await connection.OpenAsync(cancellationToken).NotOnCapturedContext();

            return connection;
        }
        
        private async Task<OracleTransaction> StartTransaction(CancellationToken cancellationToken)
        {
            var connection = await OpenConnection(cancellationToken);
            return connection.BeginTransaction(IsolationLevel.ReadCommitted);
        }
        
        private IObservable<Unit> GetStoreObservable => _streamStoreNotifier.Value;
    }
}