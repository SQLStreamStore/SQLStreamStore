namespace SqlStreamStore.Server
{
    using System;
    using System.Data;
    using System.Diagnostics;
    using System.IO;
    using System.Linq;
    using System.Net;
    using System.Threading;
    using System.Threading.Tasks;
    using MySql.Data.MySqlClient;
    using Xunit.Abstractions;

    /// <inheritdoc />
    /// <summary>
    ///     A class controlling test database initializing and cleanup
    /// </summary>
    internal class MySqlServer : IDisposable
    {
        private readonly ITestOutputHelper _testOutputHelper;
        public const int DefaultPort = 3306;
        private readonly string _dataDirectory;
        private Process _process;
        private readonly string[] _mysqldArguments;
        private readonly string _mysqldPath;
        private readonly string _mysqlDataDirectory;
        private bool _started;

        /// <summary>
        ///     The MySQL server is started in the constructor
        /// </summary>
        public MySqlServer(ITestOutputHelper testOutputHelper, int serverPort = DefaultPort)
        {
            _testOutputHelper = testOutputHelper ?? throw new ArgumentNullException(nameof(testOutputHelper));
            if(serverPort < 1024 || serverPort > ushort.MaxValue)
            {
                throw new ArgumentOutOfRangeException(nameof(serverPort));
            }
            var mysqlDirectory = Path.Combine(AppDomain.CurrentDomain.BaseDirectory, ".mysql");
            _dataDirectory = Path.Combine(Path.GetTempPath(), Guid.NewGuid().ToString("n"));
            _mysqldPath = Path.Combine(mysqlDirectory, "mysqld.exe");
            _mysqlDataDirectory = Path.Combine(mysqlDirectory, "data", "mysql");

            ServerPort = serverPort;

            Directory.CreateDirectory(_dataDirectory);

            _process = new Process();

            _mysqldArguments = new[]
            {
                "--standalone",
                $@"--basedir=""{mysqlDirectory}""",
                $@"--lc-messages-dir=""{mysqlDirectory}""",
                $@"--datadir=""{_dataDirectory}""",
                "--skip-grant-tables",
                $"--bind-address={IPAddress.Loopback}",
                $"--port={ServerPort}",
                "--innodb_fast_shutdown=2",
                "--innodb_doublewrite=OFF",
                "--innodb_log_file_size=1048576",
                "--innodb_data_file_path=ibdata1:10M;ibdata2:10M:autoextend"
            };
        }

        public int ServerPort { get; }

        public int ProcessId => !_process.HasExited ? _process.Id : -1;

        /// <summary>
        ///     Get a connection string for the server (no database selected)
        /// </summary>
        /// <value>A connection string for the server</value>
        public string ConnectionString => $"Server={IPAddress.Loopback};Port={ServerPort};";

        /// <summary>
        ///     Get a connection string for the server and a specified database
        /// </summary>
        /// <param name="databaseName">The name of the database</param>
        /// <returns>A connection string for the server and database</returns>
        public string GetConnectionString(string databaseName)
            => $"Server={IPAddress.Loopback};Port={ServerPort};Database={databaseName};";

        /// <summary>
        ///     Checks if the server is started. The most reliable way is simply to check if we can connect to it
        /// </summary>
        public async Task Start(CancellationToken ct = default(CancellationToken))
        {
            var tempMysqlDataDirectory = Path.Combine(_dataDirectory, "mysql");
            
            Directory.CreateDirectory(tempMysqlDataDirectory);

            await Task.WhenAll(Directory.EnumerateFiles(_mysqlDataDirectory).Select(async path =>
            {
                using(var stream = File.OpenRead(path))
                using(var copy = File.Create(Path.Combine(tempMysqlDataDirectory, Path.GetFileName(path))))
                {
                    await stream.CopyToAsync(copy);
                }
            }));
            
            _process = new Process
            {
                StartInfo =
                {
                    UseShellExecute = false,
                    CreateNoWindow = true,
                    Arguments = string.Join(" ", _mysqldArguments),
                    FileName = _mysqldPath,
                    RedirectStandardError = true,
                    RedirectStandardOutput = true
                }
            };

            _process.ErrorDataReceived += (_, e) => _testOutputHelper.WriteLine(e.Data ?? string.Empty);
            _process.OutputDataReceived += (_, e) => _testOutputHelper.WriteLine(e.Data ?? string.Empty);

            _process.Start();
            _process.BeginErrorReadLine();
            _process.BeginOutputReadLine();

            await TestConnection(ct);

            _started = true;
        }

        private async Task TestConnection(CancellationToken ct)
        {
            using(var connection = new MySqlConnection(ConnectionString))
            {
                while(!connection.State.Equals(ConnectionState.Open))
                {
                    try
                    {
                        await connection.OpenAsync(ct);
                    }
                    catch(Exception e)
                    {
                        if(_process.HasExited)
                        {
                            throw;
                        }
                        await Task.Delay(100, ct);
                    }
                }
            }
        }

        public MySqlConnection CreateConnection(string databaseName = null)
        {
            if(!_started)
            {
                throw new InvalidOperationException("MySqlServer must be started first.");
            }

            return new MySqlConnection(databaseName == null ? ConnectionString : GetConnectionString(databaseName));
        }

        public void Dispose()
        {
            try
            {
                _process?.Kill();
            }
            catch(Exception ex)
            {
                _testOutputHelper.WriteLine($"Could not kill process while disposing: {ex}");
            }

            _process?.Dispose();

            var stopwatch = Stopwatch.StartNew();
            while(stopwatch.ElapsedMilliseconds < 1000)
            {
                try
                {
                    Directory.Delete(_dataDirectory, true);
                    break;
                }
                catch(IOException)
                {
                    Thread.Sleep(100);
                }
            }
        }
   }
}