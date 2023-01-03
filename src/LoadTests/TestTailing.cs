namespace LoadTests
{
    using System;
    using System.Collections.Generic;
    using System.Diagnostics;
    using System.Linq;
    using System.Threading;
    using System.Threading.Tasks;

    using EasyConsole;

    using Npgsql;

    using SqlStreamStore;
    using SqlStreamStore.Infrastructure;
    using SqlStreamStore.PgSqlScripts;
    using SqlStreamStore.Streams;

    public class TestTailing : LoadTest
    {
        private static readonly object s_lock = new object();
        private static readonly List<long> s_db = new List<long>();
        private static IAllStreamSubscription s_subscription;
        public override async Task Run(CancellationToken ct)
        {
            Output.WriteLine("");
            Output.WriteLine(ConsoleColor.Green, "Subscription that tails head with delayed appends.");
            Output.WriteLine("");

            const string schemaName = "tailing";
            var (streamStore, dispose) = await GetStore(ct, schemaName);

            if(streamStore is not PostgresStreamStore pgStreamStore)
            {
                Output.WriteLine($"No support for {nameof(streamStore)} for this test.");
                return;
            }

            try
            {
                var numberOfStreams = Input.ReadInt("Number of streams: ", 1, 100000000);
                int messageJsonDataSize = Input.ReadInt("Size of Json (kb): ", 1, 1024);
                int numberOfMessagesPerAmend = Input.ReadInt("Number of messages per stream append: ", 1, 1000);
                int readPageSize = Input.ReadInt("Read page size: ", 1, 10000);

                string jsonData = $@"{{""b"": ""{new string('a', messageJsonDataSize * 1024)}""}}";

                var linkedToken = CancellationTokenSource.CreateLinkedTokenSource(ct);

#pragma warning disable CS4014 // Because this call is not awaited, execution of the current method continues before the call is completed
                Task.Run(() => RunSubscribe(pgStreamStore, readPageSize), linkedToken.Token);
#pragma warning restore CS4014 // Because this call is not awaited, execution of the current method continues before the call is completed

                var sw = Stopwatch.StartNew();
                var concurrentWrite = new List<(CancellationTokenSource source, Task task)>();
                var concurrentDelayedTx = new List<(CancellationTokenSource source, Task task)>();

                var maxConcurrent = 20;
                var minConcurrent = 15;

                var up = true;

                var nextTimeLog = 300;
                var nextTimeWrites = 100;
                var nextTimeDelayTx = 750;

                while (sw.ElapsedMilliseconds < TimeSpan.FromSeconds(30).TotalMilliseconds)
                {
                    if (sw.ElapsedMilliseconds >= nextTimeLog)
                    {
                        nextTimeLog += 300;
                        var head = await pgStreamStore.ReadHeadPosition(linkedToken.Token);

                        lock (s_lock)
                        {
                            var subscriptionPosition = s_db.LastOrDefault();
                            Output.WriteLine($"Head: {head} | Subscription: {subscriptionPosition} "
                                             + $"| ConcurrentWrite: {concurrentWrite.Count(x => !x.task.IsCompleted)} "
                                             + $"| concurrentDelayedTx: {concurrentDelayedTx.Count(x => !x.task.IsCompleted)}");
                        }
                    }

                    if (sw.ElapsedMilliseconds >= nextTimeWrites)
                    {
                        nextTimeWrites += 100;
                        if (up)
                        {
                            Output.WriteLine("Adding concurrent write");

                            var cts = new CancellationTokenSource();
                            var t = Task.Run(() =>
                                    RunWrites(cts.Token,
                                        numberOfMessagesPerAmend,
                                        numberOfStreams,
                                        concurrentWrite.Count * numberOfStreams,
                                        jsonData,
                                        pgStreamStore),
                                cts.Token);
                            concurrentWrite.Add((cts, t));
                        }
                        else
                        {
                            Output.WriteLine("Cancelling concurrent write");
                            var cts = concurrentWrite.First();
                            cts.source.Cancel();
                            cts.source.Dispose();
                            concurrentWrite.Remove(cts);
                        }
                    }

                    if (sw.ElapsedMilliseconds >= nextTimeDelayTx)
                    {
                        Output.WriteLine("Adding delay write");

                        nextTimeDelayTx += 1000;
                        var cts = new CancellationTokenSource();
                        var t = Task.Run(() =>
                                AddTransaction(pgStreamStore,
                                    schemaName,
                                    3050,
                                    jsonData,
                                    numberOfMessagesPerAmend,
                                    cts.Token),
                            cts.Token);
                        concurrentDelayedTx.Add((cts, t));
                    }

                    foreach (var completedWrite in concurrentWrite.Where(x => x.task.IsCompleted).ToList())
                    {
                        completedWrite.source.Dispose();
                        concurrentWrite.Remove(completedWrite);
                    }

                    if (concurrentWrite.Count < minConcurrent)
                        up = true;
                    else if (concurrentWrite.Count > maxConcurrent)
                        up = false;
                }

                foreach (var (cancellationTokenSource, _) in concurrentWrite)
                {
                    cancellationTokenSource.Cancel();
                    cancellationTokenSource.Dispose();
                }

                foreach (var (cancellationTokenSource, _) in concurrentDelayedTx)
                {
                    cancellationTokenSource.Cancel();
                    cancellationTokenSource.Dispose();
                }

                await Task.WhenAll(concurrentWrite.Select(x => x.task).Where(t => !t.IsCompleted));
                await Task.WhenAll(concurrentDelayedTx.Select(x => x.task).Where(t => !t.IsCompleted));

                Output.WriteLine("Writes finished");

                var db = new List<long>();
                ReadAllPage page = await pgStreamStore.ReadAllForwards(Position.Start, 500, false, ct);
                do
                {
                    db.AddRange(page.Messages.Select(x => x.Position));
                    page = await page.ReadNext(linkedToken.Token);
                } while (!page.IsEnd);

                db.AddRange(page.Messages.Select(x => x.Position));

                // It's possible that s_db.Last() will never be equal to head (skipped event) hence why we time limit this loop.
                var maxLoopTime = sw.ElapsedMilliseconds + TimeSpan.FromSeconds(30).Milliseconds;
                while (sw.ElapsedMilliseconds < maxLoopTime)
                {
                    var head = await pgStreamStore.ReadHeadPosition(linkedToken.Token);
                    lock (s_lock)
                    {
                        if (head == s_db.Last())
                            break;
                    }

                    await Task.Delay(150, linkedToken.Token);
                }

                linkedToken.Cancel();
                linkedToken.Dispose();
                s_subscription.Dispose();

                lock (s_lock)
                {
                    Output.WriteLine(!db.SequenceEqual(s_db) ? "DONE WITH SKIPPED EVENT" : "Done without skipped event");
                }
            }
            finally
            {
                dispose();
            }
        }

        private static async Task AddTransaction(PostgresStreamStore pgStreamStore, string schemaName, int delayCommitTime, string jsonData, int numberOfMessagesPerAmend, CancellationToken cancellationToken)
        {
            try
            {
                const int expectedVersion = ExpectedVersion.Any;
                var streamId = $"transactiontest/{Guid.NewGuid()}";
                var schema = new Schema(schemaName);

                AppendResult result;

                var streamIdInfo = new StreamIdInfo(streamId);

                var messages = new List<NewStreamMessage>
                {
                    new NewStreamMessage(Guid.NewGuid(), "TestTransaction", "{}")
                }.ToArray();

                using (var connection = await pgStreamStore.OpenConnection(cancellationToken))
                using (var transaction = await connection.BeginTransactionAsync(cancellationToken))
                using (var command = BuildFunctionCommand(
                          schema.AppendToStream,
                          transaction,
                          Parameters.StreamId(streamIdInfo.PostgresqlStreamId),
                          Parameters.StreamIdOriginal(streamIdInfo.PostgresqlStreamId),
                          Parameters.MetadataStreamId(streamIdInfo.MetadataPosgresqlStreamId),
                          Parameters.ExpectedVersion(expectedVersion),
                          Parameters.CreatedUtc(SystemClock.GetUtcNow.Invoke()),
                          Parameters.NewStreamMessages(messages)))
                {
                    try
                    {
                        using (var reader = await command
                                  .ExecuteReaderAsync(cancellationToken)
                                  .ConfigureAwait(false))
                        {
                            await reader.ReadAsync(cancellationToken).ConfigureAwait(false);
                            result = new AppendResult(reader.GetInt32(0), reader.GetInt64(1));
                        }


                        await Task.Delay(delayCommitTime, cancellationToken);
                        await transaction.CommitAsync(cancellationToken).ConfigureAwait(false);
                    }
                    catch (PostgresException ex) when (ex.IsWrongExpectedVersion())
                    {
                        await transaction.RollbackAsync(cancellationToken).ConfigureAwait(false);

                        throw new WrongExpectedVersionException(
                            ErrorMessages.AppendFailedWrongExpectedVersion(streamIdInfo.PostgresqlStreamId.IdOriginal,
                                expectedVersion),
                            streamIdInfo.PostgresqlStreamId.IdOriginal,
                            expectedVersion,
                            ex);
                    }
                }
            }
            catch (OperationCanceledException ex)
            {
                Output.WriteLine(ex.ToString());

            }
            catch (Exception ex) when (!(ex is OperationCanceledException))
            {
                Output.WriteLine(ex.ToString());
            }
        }

        private static NpgsqlCommand BuildFunctionCommand(
            string function,
            NpgsqlTransaction transaction,
            params NpgsqlParameter[] parameters)
        {
            var command = new NpgsqlCommand(function, transaction.Connection, transaction);

            foreach (var parameter in parameters)
            {
                command.Parameters.Add(parameter);
            }

            command.BuildFunction();
            return command;
        }

        private static Task RunSubscribe(IStreamStore streamStore, int readPageSize)
        {
            s_subscription = streamStore.SubscribeToAll(
                null,
                (_, m, ___) =>
                {
                    lock (s_lock)
                    {
                        s_db.Add(m.Position);
                        return Task.CompletedTask;
                    }

                });
            s_subscription.MaxCountPerRead = readPageSize;

            return Task.CompletedTask;
        }

        private static async Task RunWrites(
            CancellationToken ct,
            int numberOfMessagesPerAmend,
            int numberOfStreams,
            int offset,
            string jsonData,
            IStreamStore streamStore)
        {
            var stopwatch = Stopwatch.StartNew();
            var messageNumbers = new int[numberOfMessagesPerAmend];
            int count = 1;
            for (int i = 0; i < numberOfStreams; i++)
            {
                try
                {
                    ct.ThrowIfCancellationRequested();

                    for (int j = 0; j < numberOfMessagesPerAmend; j++)
                    {
                        messageNumbers[j] = count++;
                    }

                    var newMessages = MessageFactory
                        .CreateNewStreamMessages(jsonData, messageNumbers);

                    await streamStore.AppendToStream(
                        $"stream-{i + offset}",
                        ExpectedVersion.Any,
                        newMessages,
                        ct);

                }
                catch (OperationCanceledException)
                {
                    //Output.WriteLine(ex.ToString());
                    break;
                }
                catch (Exception ex) when (!(ex is OperationCanceledException))
                {
                    //Output.WriteLine(ex.ToString());
                    break;
                }
            }
            stopwatch.Stop();
            var rate = Math.Round((decimal)count / stopwatch.ElapsedMilliseconds * 1000, 0);

            Output.WriteLine("");
            Output.WriteLine($"> {count - 1} messages written in {stopwatch.Elapsed} ({rate} m/s)");
        }
    }
}