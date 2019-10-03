using System;
using System.ComponentModel;
using System.IO;
using System.Linq;
using static Bullseye.Targets;
using static SimpleExec.Command;

namespace build
{
    class Program
    {
        private const string ArtifactsDir = "artifacts";
        private const string BuildHalDocs = "build-hal-docs";
        private const string Build = "build";
        private const string Test = "test";
        private const string Pack = "pack";
        private const string Publish = "publish";

        private static void Main(string[] args)
        {
            Target(BuildHalDocs, () =>
            {
                Run("yarn", workingDirectory: "./tools/hal-docs");

                var srcDirectory = new DirectoryInfo("./src");

                var schemaDirectories = srcDirectory.GetFiles("*.schema.json", SearchOption.AllDirectories)
                    .Select(schemaFile => schemaFile.DirectoryName)
                    .Distinct()
                    .Select(schemaDirectory => schemaDirectory.Replace(Path.DirectorySeparatorChar, '/'));

                foreach (var schemaDirectory in schemaDirectories)
                {
                    Run("node",
                    $"node_modules/@adobe/jsonschema2md/cli.js -n --input {schemaDirectory} --out {schemaDirectory} --schema-out=-",
                    "tools/hal-docs");
                }
            });

            Target(Build, () => Run("dotnet", "build --configuration=Release"));

            Target(
                Test,
                DependsOn(Build),
                () => Run("dotnet", "test --configuration=Release --no-build --no-restore --verbosity=normal"));

            Target(
                Pack,
                DependsOn(Build),
                ForEach(
                    "SqlStreamStore",
                    "SqlStreamStore.MsSql",
                    "SqlStreamStore.MySql",
                    "SqlStreamStore.Postgres",
                    "SqlStreamStore.HAL",
                    "SqlStreamStore.Http",
                    "SqlStreamStore.SchemaCreationScriptTool"),
                project => Run("dotnet", $"pack src/{project}/{project}.csproj -c Release -o ../../{ArtifactsDir} --no-build"));

            Target(Publish, DependsOn(Pack), () =>
            {
                var packagesToPush = Directory.GetFiles(ArtifactsDir, "*.nupkg", SearchOption.TopDirectoryOnly);
                Console.WriteLine($"Found packages to publish: {string.Join("; ", packagesToPush)}");

                var apiKey = Environment.GetEnvironmentVariable("MYGET_API_KEY");

                if (string.IsNullOrWhiteSpace(apiKey))
                {
                    Console.WriteLine("MyGet API key not available. Packages will not be pushed.");
                    return;
                }

                foreach (var packageToPush in packagesToPush)
                {
                    Run("dotnet", $"nuget push {packageToPush} -s https://www.myget.org/F/sqlstreamstore/api/v3/index.json -k {apiKey}", noEcho: true);
                }
            });

            Target("default", DependsOn(Test, Publish));

            RunTargetsAndExit(args);
        }
    }
}
