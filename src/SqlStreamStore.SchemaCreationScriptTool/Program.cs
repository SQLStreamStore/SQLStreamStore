namespace SqlStreamStore.SchemaCreationScriptTool
{
    using System;
    using System.ComponentModel.DataAnnotations;
    using System.Data.SqlClient;
    using System.IO;
    using McMaster.Extensions.CommandLineUtils;
    using MySqlConnector;
    using Npgsql;
    using Serilog;

    [Command(
        Name = "sqlstreamstore-schema-creation-script", 
        FullName = "SqlStreamStore Schema Creation Script Tool",
        Description = "A tool to generate the SQL Stream Store schema creation script of the various SQL based implementations."
    )]
    [VersionOptionFromMember("-v|--version", MemberName = nameof(Version))]
    internal class Program
    {
        [Argument(0, "sqldialect", "The SQL dialect to generate the schema creation script for: mssqlv2|mssqlv3|mysql|postgres")]
        [AllowedValues("mssqlv2", "mssqlv3", "mysql", "postgres", IgnoreCase = true)]
        public string SQLDialect { get; set; }
        
        [Option("-s|--schema <SCHEMA>", "The optional database schema name (only applies to mssql and postgres)", CommandOptionType.SingleValue)]
        public string Schema { get; set; }

        [Option("-cs|--create-schema",
            "Indicates if the schema should be created as part of generation (only applies to mssql and postgres - defaults to false)",
            CommandOptionType.NoValue)]
        public bool CreateSchema { get; set; } = false;

        [Required]
        [Option("-o|--output <PATH>", "The file path to write the schema creation script to", CommandOptionType.SingleValue)]
        public string Output { get; set; }

        private string Version { get; } = typeof(IStreamStore)
            .Assembly
            .GetName()
            .Version
            .ToString();

        private int OnExecute(CommandLineApplication app)
        {
            if(string.IsNullOrEmpty(SQLDialect) || string.IsNullOrEmpty(Output))
            {
                app.ShowHelp();
                return 0;
            }

            var exitCode = 0; 
            switch(SQLDialect.ToLowerInvariant())
            {
                case "mssqlv2":
#pragma warning disable 618
                    var mssqlV2Settings = new MsSqlStreamStoreSettings(new SqlConnectionStringBuilder
                    {
                        DataSource = "tcp:0.0.0.0,1433"
                    }.ConnectionString);
                    if(!string.IsNullOrEmpty(Schema))
                    {
                        mssqlV2Settings.Schema = Schema;
                    }

                    if(CreateSchema)
                    {
                        var script = string.Join(
                            Environment.NewLine,
                            $@"IF NOT EXISTS (
SELECT  schema_name
FROM    information_schema.schemata
WHERE   schema_name = '{Schema}' ) 

BEGIN
EXEC sp_executesql N'CREATE SCHEMA {Schema}'
END",
                            new MsSqlStreamStore(mssqlV2Settings).GetSchemaCreationScript());
                        File.WriteAllText(Output, script);
                    }
                    else
                    {
                        File.WriteAllText(Output, new MsSqlStreamStore(mssqlV2Settings).GetSchemaCreationScript());    
                    }
#pragma warning restore 618
                    break;
                case "mssqlv3": 
                    var mssqlV3Settings = new MsSqlStreamStoreV3Settings(new SqlConnectionStringBuilder
                    {
                        DataSource = "tcp:0.0.0.0,1433"
                    }.ConnectionString);
                    if(!string.IsNullOrEmpty(Schema))
                    {
                        mssqlV3Settings.Schema = Schema;
                    }
                    if(CreateSchema)
                    {
                        var script = string.Join(
                            Environment.NewLine,
                            $@"IF NOT EXISTS (
SELECT  schema_name
FROM    information_schema.schemata
WHERE   schema_name = '{Schema}' ) 

BEGIN
EXEC sp_executesql N'CREATE SCHEMA {Schema}'
END",
                            new MsSqlStreamStoreV3(mssqlV3Settings).GetSchemaCreationScript());
                        File.WriteAllText(Output, script);
                    }
                    else
                    {
                        File.WriteAllText(Output, new MsSqlStreamStoreV3(mssqlV3Settings).GetSchemaCreationScript());
                    }
                    
                    break;
                case "mysql": 
                    var mysqlSettings = new MySqlStreamStoreSettings(new MySqlConnectionStringBuilder
                    {
                        Server = "0.0.0.0"
                    }.ConnectionString);
                    if(!string.IsNullOrEmpty(Schema))
                    {
                        Log.Information("The optional database schema does not apply to the mysql dialect and can be omitted: {Schema}", Schema);
                    }
                    File.WriteAllText(Output, new MySqlStreamStore(mysqlSettings).GetSchemaCreationScript());
                    break;
                case "postgres": 
                    var postgresSettings = new PostgresStreamStoreSettings(new NpgsqlConnectionStringBuilder
                    {
                        Host = "0.0.0.0"
                    }.ConnectionString);
                    if(!string.IsNullOrEmpty(Schema))
                    {
                        postgresSettings.Schema = Schema;
                    }

                    if(CreateSchema)
                    {
                        var script = string.Join(
                            Environment.NewLine,
                            $"CREATE SCHEMA IF NOT EXISTS {Schema};",
                            new PostgresStreamStore(postgresSettings).GetSchemaCreationScript()
                        );
                        File.WriteAllText(Output, script);
                    }
                    else
                    {
                        File.WriteAllText(Output, new PostgresStreamStore(postgresSettings).GetSchemaCreationScript());                        
                    }
                    break;
                default:
                    Log.Error("The SQL dialect was not recognized: {SQLDialect}", SQLDialect);
                    exitCode = 1;
                    break;
            }

            return exitCode;
        }
        
        public static int Main(string[] args)
        {
            Log.Logger = new LoggerConfiguration()
                .MinimumLevel.Debug()
                .Enrich.FromLogContext()
                .WriteTo.Console()
                .CreateLogger();
            
            return CommandLineApplication.Execute<Program>(args);
        }
    }
}
